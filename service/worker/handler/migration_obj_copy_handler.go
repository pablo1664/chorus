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
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/service/worker/copy"
)

func (s *svc) HandleMigrationObjCopy(ctx context.Context, t *asynq.Task) (err error) {
	var p tasks.MigrateObjCopyPayload
	if err = json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("MigrateObjCopyPayload Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}
	ctx = log.WithBucket(ctx, p.Bucket)
	ctx = log.WithObjName(ctx, p.Obj.Name)
	logger := zerolog.Ctx(ctx)
	fromBucket, toBucket := p.ID.FromToBuckets(p.Bucket)

	// acquire rate limits for source and destination storage before proceeding
	if err := s.rateLimit(ctx, p.ID.FromStorage(), s3.HeadObject, s3.GetObject, s3.GetObjectAcl); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ID.FromStorage()).Msg("rate limit error")
		return err
	}
	if err := s.rateLimit(ctx, p.ID.ToStorage(), s3.HeadObject, s3.PutObject, s3.PutObjectAcl); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ID.ToStorage()).Msg("rate limit error")
		return err
	}

	domObj := dom.Object{
		Bucket:  p.Bucket,
		Name:    p.Obj.Name,
		Version: p.Obj.VersionID,
	}

	objectLockID := entity.NewVersionedObjectLockID(p.ID.ToStorage(), toBucket, p.Obj.Name, p.Obj.VersionID)
	lock, err := s.objectLocker.Lock(ctx, objectLockID)
	if err != nil {
		return err
	}
	defer lock.Release(ctx)
	versions, err := s.versionSvc.GetObj(ctx, p.ID, domObj)
	if err != nil {
		return fmt.Errorf("migration obj copy: unable to get obj meta: %w", err)
	}
	fromVer, toVer := versions.From, versions.To

	if fromVer != 0 && fromVer <= toVer {
		logger.Info().Int("from_ver", fromVer).Int("to_ver", toVer).Msg("migration obj copy: identical from/to obj version: skip copy")
		return nil
	}
	// 1. sync obj meta and content
	err = lock.Do(ctx, time.Second*2, func() error {
		return s.copySvc.CopyObject(ctx, p.ID.User(), copy.File{
			Storage: p.ID.FromStorage(),
			Bucket:  fromBucket,
			Name:    p.Obj.Name,
		}, copy.File{
			Storage: p.ID.ToStorage(),
			Bucket:  toBucket,
			Name:    p.Obj.Name,
		})
	})
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			// Some S3/Ceph implementations list directory markers (size=0, key ending
			// with "/") in ListObjects but return 404 on StatObject. In that case,
			// create an empty marker directly on the destination instead of skipping.
			if p.Obj.Size == 0 && strings.HasSuffix(p.Obj.Name, "/") {
				if dirErr := s.putDirMarker(ctx, p, toBucket); dirErr != nil {
					return fmt.Errorf("migration obj copy: unable to create dir marker on destination: %w", dirErr)
				}
				logger.Info().Msg("migration obj copy: dir marker created on destination")
				return nil
			}
			logger.Warn().Msg("migration obj copy: skip object sync: object missing in source")
			return nil
		}
		return fmt.Errorf("migration obj copy: unable to copy object: %w", err)
	}

	if fromVer != 0 {
		destination := meta.Destination{Storage: p.ID.ToStorage(), Bucket: toBucket}
		err = s.versionSvc.UpdateIfGreater(ctx, p.ID, domObj, destination, fromVer)
		if err != nil {
			return fmt.Errorf("migration obj copy: unable to update obj meta: %w", err)
		}
	}
	logger.Info().Msg("migration obj copy: done")

	return nil
}

// putDirMarker creates an empty directory marker object on the destination.
// Used when the source lists a directory marker but StatObject returns 404 (common in Ceph/RGW).
func (s *svc) putDirMarker(ctx context.Context, p tasks.MigrateObjCopyPayload, toBucket string) error {
	toClient, err := s.clients.AsS3(ctx, p.ID.ToStorage(), p.ID.User())
	if err != nil {
		return err
	}
	_, err = toClient.S3().PutObject(ctx, toBucket, p.Obj.Name, strings.NewReader(""), 0, mclient.PutObjectOptions{
		ContentType:          "application/x-directory",
		DisableContentSha256: true,
	})
	return err
}
