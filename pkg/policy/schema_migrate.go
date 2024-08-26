package policy

import (
	"context"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
)

var Migrations = []func(ctx context.Context, client *redis.Client) error{
	migrateRouting,
	migrateReplication,
}

func migrateRouting(ctx context.Context, client *redis.Client) error {
	pipe := client.Pipeline()
	iter := client.Scan(ctx, 0, "p:route:*", 0).Iterator()
	var keys []string
	var vals []*redis.StringCmd
	for iter.Next(ctx) {
		key := iter.Val()
		keys = append(keys, key)
		vals = append(vals, pipe.Get(ctx, key))
	}
	if err := iter.Err(); err != nil {
		return err
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		return err
	}
	pipe = client.Pipeline()
	for i, key := range keys {
		val, err := vals[i].Result()
		if err != nil {
			return err
		}
		id := strings.TrimPrefix(key, "p:route:")
		ids := strings.Split(id, ":")
		if len(ids) != 2 {
			continue
		}
		newID := BucketID{
			Account: "",
			Bucket:  ids[1],
		}
		// create new routing policy
		_ = pipe.Set(ctx, newID.bucketRoutingPolicyID(), val, 0)
		// delete old routing policy
		_ = pipe.Del(ctx, key)
	}
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("%w: unable to exec routing migration update pipe", err)
	}
	return nil
}

func migrateReplication(ctx context.Context, client *redis.Client) error {
	pipe := client.Pipeline()
	iter := client.Scan(ctx, 0, "p:repl_st:*", 0).Iterator()
	var keys []string
	for iter.Next(ctx) {
		key := iter.Val()
		vals := strings.Split(key, ":")
		if len(vals) != 6 {
			zerolog.Ctx(ctx).Warn().Msgf("unable to migrate replication %s: skip", key)
			continue
		}
		bucket, from, to := vals[3], vals[4], vals[5]
		id := ReplID{
			Src: StorageBucketID{
				Storage: from,
				BucketID: BucketID{
					Account: "",
					Bucket:  bucket,
				},
			},
			Dest: StorageBucketID{
				Storage: to,
				BucketID: BucketID{
					Account: "",
					Bucket:  bucket,
				},
			},
		}
		_ = pipe.Rename(ctx, key, id.statusKey())
		keys = append(keys, key)
		vals = append(vals, pipe.Get(ctx, key))
	}
	if err := iter.Err(); err != nil {
		return err
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		return err
	}
	pipe = client.Pipeline()
	for i, key := range keys {
		val, err := vals[i].Result()
		if err != nil {
			return err
		}
		id := strings.TrimPrefix(key, "p:route:")
		ids := strings.Split(id, ":")
		if len(ids) != 2 {
			continue
		}
		newID := BucketID{
			Account: "",
			Bucket:  ids[1],
		}
		// create new routing policy
		_ = pipe.Set(ctx, newID.bucketRoutingPolicyID(), val, 0)
		// delete old routing policy
		_ = pipe.Del(ctx, key)
	}
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("%w: unable to exec routing migration update pipe", err)
	}
	return nil
}
