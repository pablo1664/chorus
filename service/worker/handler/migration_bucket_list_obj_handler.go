/*
 * Copyright © 2024 Clyso GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hibiken/asynq"
	mclient "github.com/minio/minio-go/v7"
	"github.com/rs/zerolog"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/features"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/tasks"
)

const (
	// cBackpressureThreshold is the maximum number of unprocessed copy tasks allowed in the
	// migration copy queue before the listing handler pauses to let workers catch up.
	// Keeping the queue bounded prevents Redis from accumulating tens of millions of keys
	// which degrades overall Redis performance.
	cBackpressureThreshold = 500_000
	// cBackpressureCheckInterval controls how often (in listed objects) the copy queue depth
	// is checked. Checking every object would be too expensive; every 10k is a good balance.
	cBackpressureCheckInterval = 10_000
	cBackpressureRetryIn       = 30 * time.Second
)

func (s *svc) HandleMigrationBucketListObj(ctx context.Context, t *asynq.Task) error {
	// todo: aggregate task to not list multiple times
	var p tasks.MigrateBucketListObjectsPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("HandleMigrationBucketListObj Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}
	ctx = log.WithBucket(ctx, p.Bucket)
	logger := zerolog.Ctx(ctx)
	// acquire rate limits for source storage before proceeding
	if err := s.rateLimit(ctx, p.ID.FromStorage(), s3.ListObjects); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ID.FromStorage()).Msg("rate limit error")
		return err
	}

	replicationID := p.GetReplicationID()

	// Check copy queue depth before starting to list. Return early if the queue is already
	// saturated so that copy workers get a chance to drain it before more tasks are added.
	copyQueue := tasks.InitMigrationCopyQueue(replicationID)
	if err := s.checkCopyQueueBackpressure(ctx, copyQueue); err != nil {
		return err
	}

	fromClient, err := s.clients.AsS3(ctx, p.ID.FromStorage(), p.ID.User())
	if err != nil {
		return fmt.Errorf("migration bucket list obj: unable to get %q s3 client: %w: %w", p.ID.FromStorage(), err, asynq.SkipRetry)
	}

	migrationID := entity.NewMigrationObjectIDFromUniversalReplicationID(p.ID, p.Bucket, p.Prefix)
	lastObjName, err := s.listStateStore.Get(ctx, migrationID)
	if err != nil && !errors.Is(err, dom.ErrNotFound) {
		return fmt.Errorf("unable to get last listed object: %w", err)
	}

	objects := fromClient.S3().ListObjects(ctx, p.Bucket, mclient.ListObjectsOptions{StartAfter: lastObjName, Prefix: p.Prefix})
	objectsNum := 0
	for object := range objects {
		if object.Err != nil {
			return fmt.Errorf("migration bucket list obj: list objects error %w", object.Err)
		}
		objectsNum++
		// Periodically re-check copy queue depth inside the loop so that a long-running
		// listing task does not enqueue millions of copy tasks in one shot.
		if objectsNum%cBackpressureCheckInterval == 0 {
			if bpErr := s.checkCopyQueueBackpressure(ctx, copyQueue); bpErr != nil {
				return bpErr
			}
		}
		isDir := object.Size == 0 && strings.HasSuffix(object.Key, "/")
		logger.Debug().Str(log.Object, object.Key).Str("obj_version_id", object.VersionID).Bool("is_dir", isDir).Msg("migration bucket list obj: start processing object from the list")
		tasksToEnqueue, dirObj := tasksForListedObject(ctx, p, object, replicationID)
		for _, task := range tasksToEnqueue {
			err = s.queueSvc.EnqueueTask(ctx, task)
			if err != nil {
				return fmt.Errorf("migration bucket list obj: unable to enqueue task: %w", err)
			}
		}
		if err = s.listStateStore.Set(ctx, migrationID, object.Key); err != nil {
			return fmt.Errorf("migration bucket list obj: unable to update last obj meta: %w", err)
		}
		if dirObj {
			continue
		}
	}

	if lastObjName == "" && objectsNum == 0 && p.Prefix != "" {
		// copy empty dir object
		task := tasks.MigrateObjCopyPayload{
			Bucket: p.Bucket,
			Obj: tasks.ObjPayload{
				Name: p.Prefix,
			},
		}
		task.SetReplicationID(replicationID)
		err = s.queueSvc.EnqueueTask(ctx, task)
		if err != nil {
			return fmt.Errorf("migration bucket list obj: unable to enqueue copy obj task: %w", err)
		}
	}
	_, _ = s.listStateStore.Drop(ctx, migrationID)

	logger.Info().Msg("migration bucket list obj: done")
	return nil
}

func tasksForListedObject(ctx context.Context, p tasks.MigrateBucketListObjectsPayload, object mclient.ObjectInfo, replicationID entity.UniversalReplicationID) ([]any, bool) {
	isDir := object.Size == 0 && strings.HasSuffix(object.Key, "/")
	if isDir {
		res := make([]any, 0, 2)
		if features.DirectoryMarkers(ctx) {
			if p.Versioned {
				task := tasks.ListObjectVersionsPayload{
					Bucket: p.Bucket,
					Prefix: object.Key,
				}
				task.SetReplicationID(replicationID)
				res = append(res, task)
			} else {
				task := tasks.MigrateObjCopyPayload{
					Bucket: p.Bucket,
					Obj: tasks.ObjPayload{
						Name:        object.Key,
						VersionID:   object.VersionID,
						ETag:        object.ETag,
						Size:        object.Size,
						ContentType: object.ContentType,
					},
				}
				task.SetReplicationID(replicationID)
				res = append(res, task)
			}
		}
		subP := tasks.MigrateBucketListObjectsPayload{Bucket: p.Bucket, Prefix: object.Key, Versioned: p.Versioned}
		subP.SetReplicationID(replicationID)
		res = append(res, subP)

		return res, true
	}

	if p.Versioned {
		task := tasks.ListObjectVersionsPayload{
			Bucket: p.Bucket,
			Prefix: object.Key,
		}
		task.SetReplicationID(replicationID)
		return []any{task}, false
	}

	task := tasks.MigrateObjCopyPayload{
		Bucket: p.Bucket,
		Obj: tasks.ObjPayload{
			Name:        object.Key,
			VersionID:   object.VersionID,
			ETag:        object.ETag,
			Size:        object.Size,
			ContentType: object.ContentType,
		},
	}
	task.SetReplicationID(replicationID)
	return []any{task}, false
}

// checkCopyQueueBackpressure returns ErrRateLimitExceeded if the migration copy queue has
// more than cBackpressureThreshold unprocessed tasks. Errors querying queue stats are logged
// and ignored so that a Redis hiccup does not stall migration permanently.
func (s *svc) checkCopyQueueBackpressure(ctx context.Context, copyQueue string) error {
	stats, err := s.queueSvc.Stats(ctx, copyQueue)
	if err != nil {
		if !errors.Is(err, dom.ErrNotFound) {
			zerolog.Ctx(ctx).Warn().Err(err).Str("queue", copyQueue).Msg("migration list obj: failed to get copy queue stats for backpressure check")
		}
		return nil
	}
	if stats.Unprocessed > cBackpressureThreshold {
		zerolog.Ctx(ctx).Info().
			Int("unprocessed", stats.Unprocessed).
			Int("threshold", cBackpressureThreshold).
			Str("queue", copyQueue).
			Msg("migration list obj: copy queue backpressure — pausing listing, will retry")
		return &dom.ErrRateLimitExceeded{RetryIn: cBackpressureRetryIn}
	}
	return nil
}
