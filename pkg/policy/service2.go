package policy

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
)

const (
	// replSrcIdxKey - redis key to replication sources index represented as SortedSet.
	// Where member is dest id and member score is number of destinations.
	replSrcIdxKey = "p:repl-idx:src"
	// replDestIdxKey - redis key for replication destinations index represented as Set.
	// Where each set member is destination id.
	replDestIdxKey = "p:repl-idx:dest"
)

// BucketID - points to a group of objects in terms of object storage (bucket, container, directory)
type BucketID struct {
	// Account - for SWIFT - account, for S3 - empty.
	// Represents tenant containing unique buckets
	Account string
	// Bucket - for S3 - bucket, for SWIFT - container.
	// Reprisents logical goup of objects.
	Bucket string
}

func (b BucketID) validate() error {
	if strings.Contains(b.Account, ":") {
		return fmt.Errorf("%w: Account name cannot contain ':'", dom.ErrInvalidArg)
	}
	if strings.Contains(b.Bucket, ":") {
		return fmt.Errorf("%w: Bucket name cannot contain ':'", dom.ErrInvalidArg)
	}
	return nil
}

func (b BucketID) String() string {
	return fmt.Sprintf("%s:%s", b.Account, b.Bucket)
}

func (b BucketID) bucketRoutingPolicyID() string {
	return fmt.Sprintf("p:route:%s", b.String())
}

func (b BucketID) accountRoutingPolicyID() string {
	b.Bucket = ""
	return fmt.Sprintf("p:route:%s", b.String())
}

func newBucketID(s string) BucketID {
	parts := strings.Split(s, ":")
	if len(parts) != 2 {
		return BucketID{}
	}
	return BucketID{
		Account: parts[0],
		Bucket:  parts[1],
	}
}

// StorageBucketID  - points to a Bucket (see BucketID) in a concrete Storage backend from Chorus config.
type StorageBucketID struct {
	// Storage - storage id from Chorus config.
	Storage string
	BucketID
}

func (b StorageBucketID) validate() error {
	if err := b.BucketID.validate(); err != nil {
		return err
	}
	if strings.Contains(b.Storage, ":") {
		return fmt.Errorf("%w: storage name cannot contain ':'", dom.ErrInvalidArg)
	}
	return nil
}

func (b StorageBucketID) bucketReplID() string {
	return fmt.Sprintf("p:repl:%s", b.String())
}

func (b StorageBucketID) accountReplID() string {
	b.Bucket = ""
	return fmt.Sprintf("p:repl:%s", b.String())
}

func (b StorageBucketID) storageReplID() string {
	b.Bucket = ""
	b.Account = ""
	return fmt.Sprintf("p:repl:%s", b.String())
}

func (b StorageBucketID) String() string {
	return fmt.Sprintf("%s:%s", b.Storage, b.BucketID.String())
}

type ReplDest struct {
	Priority tasks.Priority
	ID       StorageBucketID
}

type ReplID struct {
	Src, Dest StorageBucketID
}

func (r ReplID) String() string {
	return r.Src.String() + "->" + r.Dest.String()
}

func (r ReplID) validate() error {
	if err := r.Src.validate(); err != nil {
		return fmt.Errorf("%w: invalid src", err)
	}
	if err := r.Dest.validate(); err != nil {
		return fmt.Errorf("%w: invalid dest", err)
	}
	if r.Src.Account == "" && r.Dest.Account != "" {
		return fmt.Errorf("%w: invalid dest: if src acc is not set, then dest acc should also be empty ", dom.ErrInvalidArg)
	}
	if r.Dest.Account == "" && r.Src.Account != "" {
		return fmt.Errorf("%w: invalid dest: if dest acc is not set, then src acc should also be empty ", dom.ErrInvalidArg)
	}
	if r.Src.Bucket == "" && r.Dest.Bucket != "" {
		return fmt.Errorf("%w: invalid dest: if src bucket is not set, then dest bucket should also be empty ", dom.ErrInvalidArg)
	}
	if r.Dest.Bucket == "" && r.Src.Bucket != "" {
		return fmt.Errorf("%w: invalid dest: if dest bucket is not set, then src bucket should also be empty ", dom.ErrInvalidArg)
	}
	if r.Src.Storage == r.Dest.Storage && r.Src.Bucket == r.Dest.Bucket && r.Src.Account == r.Dest.Account {
		return fmt.Errorf("%w: cannot replicate to itself", dom.ErrInvalidArg)
	}
	return nil
}

type Service2 interface {
	// GetRoutingPolicy returns routing destination storage id from chorus config based on request bucket.
	// If no bucket policy found, fallbacks to Account default policy and then to main storage from config.
	// Possible errors:
	//  dom.ErrBlocked - routing for given bucket is blocked.
	GetRoutingPolicy(ctx context.Context, srcID BucketID) (string, error)
	// AddRoutingPolicy - configures routing for given bucket and account.
	// If soruce bucket is not provided, then routing will be set on Account level.
	// Possible errors:
	//  dom.ErrAlreadyExists - routing policy already exists.
	//  dom.ErrInvalidArg - invalid arguments.
	// todo: check what happens with existing repl policy
	AddRoutingPolicy(ctx context.Context, srcID BucketID, routeToStorageID string) error
	// DeleteRoutingPolicy deletes given routing policy.
	// Possible errors:
	//  dom.ErrNotFound - when policy is not found.
	// todo: check what happens with corresponding repl policy
	DeleteRoutingPolicy(ctx context.Context, srcID BucketID) error

	// ListBlockedBuckets returns bucket filtered by account with blocked routing policy.
	// Retruns all blocked buckets if no account provided.
	ListBlockedBuckets(ctx context.Context, account string) (map[string]struct{}, error)

	// GetBucketReplicationPolicies returns destinations for bucket replication.
	// If no bucket replication policy found, fallbacks to Account or Storage default Replication if exists.
	// Possible errors:
	//  dom.ErrInvalidArg - invalid arguments.
	//  dom.ErrNotFound - no replication policies found.
	GetBucketReplicationPolicies(ctx context.Context, srcID StorageBucketID) ([]ReplDest, error)
	// AddReplicationPolicy - adds replication policy.
	// id.Src can be defined partially - omit src.Bucket to create Storage or Account level default policy.
	// id.Src should not conflict with routing policy.
	// Possible errors:
	//  dom.ErrAlreadyExists - replication policy already exists.
	//  dom.ErrInvalidArg - invalid arguments.
	AddReplicationPolicy(ctx context.Context, id ReplID, priority tasks.Priority) error
	// DeleteReplicationPolicy - deletes corresponig policy.
	// Possible errors:
	//  dom.ErrNotFound - no replication policies found.
	DeleteReplicationPolicy(ctx context.Context, id ReplID) error

	// AddReplicationInfo - creates mutable metadata for bucket replication policy.
	AddReplicationInfo(ctx context.Context, id ReplID, agentURL *string) error

	// GetReplicationPolicyInfo - returns replication status.
	// Possible errors:
	//  dom.ErrNotFound - no replication policy found.
	GetReplicationPolicyInfo(ctx context.Context, id ReplID) (ReplicationPolicyStatus, error)
	// ListReplicationPolicyInfo - returns all replication policies with status.
	ListReplicationPolicyInfo(ctx context.Context) ([]ReplicationPolicyStatusExtended, error)
	// IsReplicationPolicyExists - checks if given policy exists.
	IsReplicationPolicyExists(ctx context.Context, id ReplID) (bool, error)
	// IsReplicationPolicyPaused - returns true if given replication is paused.
	// Possible errors:
	//  dom.ErrNotFound - no replication policy found.
	IsReplicationPolicyPaused(ctx context.Context, id ReplID) (bool, error)
	// IncReplInitObjListed - atomically increase listed existing objects number for given replication.
	// Possible errors:
	//  dom.ErrNotFound - no replication policy found.
	IncReplInitObjListed(ctx context.Context, id ReplID, bytes int64, eventTime time.Time) error
	// IncReplInitObjDone - atomically increase synced existing objects number for given replication.
	// Possible errors:
	//  dom.ErrNotFound - no replication policy found.
	IncReplInitObjDone(ctx context.Context, id ReplID, bytes int64, eventTime time.Time) error
	// ObjListStarted - returns true if replication process is already started.
	// Possible errors:
	//  dom.ErrNotFound - no replication policy found.
	ObjListStarted(ctx context.Context, id ReplID) error
	// IncReplEvents - atomically increase number of write events to replication source.
	// Possible errors:
	//  dom.ErrNotFound - no replication policy found.
	IncReplEvents(ctx context.Context, id ReplID, eventTime time.Time) error
	// IncReplEventsDone - atomically increase number of synced source changes.
	// Possible errors:
	//  dom.ErrNotFound - no replication policy found.
	IncReplEventsDone(ctx context.Context, id ReplID, eventTime time.Time) error
	// PauseReplication - pauses corresponig policy.
	// Possible errors:
	//  dom.ErrNotFound - no replication policy found.
	PauseReplication(ctx context.Context, id ReplID) error
	// ResumeReplication - resumes corresponig policy.
	// Possible errors:
	//  dom.ErrNotFound - no replication policy found.
	ResumeReplication(ctx context.Context, id ReplID) error

	IsReplicationSwitchInProgress(ctx context.Context, src StorageBucketID) (bool, error)
	GetReplicationSwitch(ctx context.Context, src StorageBucketID) (ReplicationSwitch, error)
	DoReplicationSwitch(ctx context.Context, id ReplID) error
	ReplicationSwitchDone(ctx context.Context, src StorageBucketID) error

	// todo: refactor
	// DeleteBucketReplicationsByUser(ctx context.Context, user, from string, to string) ([]string, error)
}

func NewSvc2(storages map[string]bool, client *redis.Client) *policySvc2 {
	res := policySvc2{
		client:   client,
		storages: storages,
	}
	for name, main := range storages {
		if main {
			res.mainStorage = name
			break
		}
	}
	return &res
}

var _ Service2 = &policySvc2{}

type policySvc2 struct {
	client      *redis.Client
	storages    map[string]bool
	mainStorage string
}

func (s *policySvc2) GetRoutingPolicy(ctx context.Context, srcID BucketID) (string, error) {
	bKey, aKey := srcID.bucketRoutingPolicyID(), srcID.accountRoutingPolicyID()
	pipe := s.client.Pipeline()
	bRes, aRes := pipe.Get(ctx, bKey), pipe.Get(ctx, aKey)
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return "", err
	}

	parseFn := func(res *redis.StringCmd, key string) (string, error) {
		route, err := res.Result()
		if err != nil {
			if err != redis.Nil {
				zerolog.Ctx(ctx).Err(err).Msgf("unable to get redis key %s", key)
			}
			return route, nil
		}

		// policy found:
		if route == routingBlock {
			return "", dom.ErrRoutingBlocked
		}
		if _, ok := s.storages[route]; !ok {
			return "", fmt.Errorf("%w: routing policy %q points to unknown storage %q", dom.ErrInternal, key, route)
		}
		return route, nil
	}

	// get bucket policy key
	route, err := parseFn(bRes, bKey)
	if err != nil {
		return "", err
	}
	if route != "" {
		return route, nil
	}

	// bucket routing not found -> fallback to account routing policy
	route, err = parseFn(aRes, aKey)
	if err != nil {
		return "", err
	}
	if route != "" {
		return route, nil
	}

	// fallback to main storage
	return s.mainStorage, nil
}

func (s *policySvc2) AddRoutingPolicy(ctx context.Context, srcID BucketID, routeToStorageID string) error {
	if err := srcID.validate(); err != nil {
		return err
	}
	if srcID.Account == "" && srcID.Bucket == "" {
		return fmt.Errorf("%w: cannot override default routing policy", dom.ErrInvalidArg)
	}
	if _, ok := s.storages[routeToStorageID]; !ok {
		return fmt.Errorf("%w: unknown storage id: %s", dom.ErrInvalidArg, routeToStorageID)
	}

	key := srcID.bucketRoutingPolicyID()
	set, err := s.client.SetNX(ctx, key, routeToStorageID, 0).Result()
	if err != nil {
		return err
	}
	if !set {
		return fmt.Errorf("%w: routing policy already exists: %s", dom.ErrAlreadyExists, key)
	}
	return nil
}

func (s *policySvc2) DeleteRoutingPolicy(ctx context.Context, srcID BucketID) error {
	pipe := s.client.Pipeline()
	routeDelRes := pipe.Del(ctx, srcID.bucketRoutingPolicyID())
	setDelRes := pipe.SRem(ctx, routingBlockSetKey, srcID.String())
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}

	deleted, err := routeDelRes.Result()
	if err != nil {
		return err
	}
	if deleted != 1 {
		return dom.ErrNotFound
	}
	if err := setDelRes.Err(); err != nil {
		zerolog.Ctx(ctx).Err(err).Msgf("unable to delete bucket block %s from set %s", srcID.String(), routingBlockSetKey)
	}
	return nil
}

func (s *policySvc2) addRoutingBlockPolicy(ctx context.Context, srcID BucketID) (err error) {
	if srcID.Bucket == "" {
		return fmt.Errorf("%w: routing block policy can be set only per bucket", dom.ErrInvalidArg)
	}
	key := srcID.bucketRoutingPolicyID()
	set := false
	set, err = s.client.SetNX(ctx, key, routingBlock, 0).Result()
	if err != nil {
		return err
	}
	if !set {
		return fmt.Errorf("%w: routing policy already exists: %s", dom.ErrAlreadyExists, key)
	}
	// rollback
	defer func() {
		if err == nil {
			// no tollback needed
			return
		}
		err = s.client.Del(context.Background(), key).Err()
		if err != nil {
			zerolog.Ctx(ctx).Err(err).Msgf("unable to rollback block policy key %s", key)
		}
	}()

	// maintain list of blocked bucket to filter ListBuckets in chorus proxy.
	err = s.client.SAdd(ctx, routingBlockSetKey, srcID.String()).Err()
	return
}

func (s *policySvc2) ListBlockedBuckets(ctx context.Context, account string) (map[string]struct{}, error) {
	res, err := s.client.SMembers(ctx, routingBlockSetKey).Result()
	if err != nil {
		return nil, err
	}
	buckets := make(map[string]struct{})

	for _, v := range res {
		id := newBucketID(v)
		if account != "" && account != id.Account {
			continue
		}
		buckets[id.Bucket] = struct{}{}
	}
	return buckets, nil
}

func (s *policySvc2) GetBucketReplicationPolicies(ctx context.Context, srcID StorageBucketID) ([]ReplDest, error) {
	if _, ok := s.storages[srcID.Storage]; !ok {
		return nil, fmt.Errorf("%w: unable to ge repl policy: unknown storage %q", dom.ErrInvalidArg, srcID.Storage)
	}
	if srcID.Bucket == "" {
		return nil, fmt.Errorf("%w: unable to ge repl policy: bucket name required", dom.ErrInvalidArg)
	}

	pipe := s.client.Pipeline()
	// lookup policies with fallback
	results := []*redis.ZSliceCmd{
		pipe.ZRangeWithScores(ctx, srcID.bucketReplID(), 0, -1),  // bucket level
		pipe.ZRangeWithScores(ctx, srcID.accountReplID(), 0, -1), // account level
		pipe.ZRangeWithScores(ctx, srcID.storageReplID(), 0, -1), // storage level
	}
	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return nil, err
	}
	var destinations []ReplDest
	for _, res := range results {
		if res.Err() != nil {
			continue
		}
		if len(res.Val()) == 0 {
			continue
		}

		for _, v := range res.Val() {
			destStr, ok := v.Member.(string)
			if !ok {
				zerolog.Ctx(ctx).Error().Msgf("invalid replication destination format %+v", v.Member)
				continue
			}
			destArr := strings.Split(destStr, ":")
			if len(destArr) != 3 {
				zerolog.Ctx(ctx).Error().Msgf("invalid replication destination format %s", destStr)
				continue
			}
			storage, account, bucket := destArr[0], destArr[1], destArr[2]
			priority := tasks.Priority(v.Score)
			if priority > tasks.PriorityHighest5 {
				zerolog.Ctx(ctx).Error().Msgf("invalid replication destination priority %v", priority)
				priority = tasks.PriorityDefault1
			}
			dest := ReplDest{
				Priority: priority,
				ID: StorageBucketID{
					Storage: storage,
					BucketID: BucketID{
						Account: account,
						Bucket:  bucket,
					},
				},
			}
			if dest.ID.Bucket == "" {
				// copy values from source for storage and account level policies:
				dest.ID.Bucket = srcID.Bucket
				if dest.ID.Account == "" {
					dest.ID.Account = srcID.Account
				}
			}
			destinations = append(destinations, dest)
		}
		if len(destinations) != 0 {
			// found
			break
		}
		// fallback to next policy level
	}
	if len(destinations) == 0 {
		return nil, dom.ErrNotFound
	}
	return destinations, nil
}

func (s *policySvc2) AddReplicationPolicy(ctx context.Context, id ReplID, priority tasks.Priority) (err error) {
	if _, ok := s.storages[id.Src.Storage]; !ok {
		return fmt.Errorf("%w: unable to create replication policy: unknown source storage %s", dom.ErrInvalidArg, id.Src.Storage)
	}
	if _, ok := s.storages[id.Dest.Storage]; !ok {
		return fmt.Errorf("%w: unable to create replication policy: unknown destination storage %s", dom.ErrInvalidArg, id.Dest.Storage)
	}
	if err := id.validate(); err != nil {
		return fmt.Errorf("%w: unable to create replication policy: invalid id", err)
	}

	routing, err := s.GetRoutingPolicy(ctx, id.Src.BucketID)
	if err != nil {
		return err
	}
	if routing != id.Src.Storage {
		return fmt.Errorf("%w: replication source storage %s is different from routing storage %s", dom.ErrInvalidArg, id.Src.Storage, routing)
	}

	// check if src is already used as dst in different policy -> cascading replication is not allowed
	pipe := s.client.Pipeline()
	boolRes := []*redis.BoolCmd{
		pipe.SIsMember(ctx, replDestIdxKey, id.Src.bucketReplID()),  // bucket level
		pipe.SIsMember(ctx, replDestIdxKey, id.Src.accountReplID()), // account level
		pipe.SIsMember(ctx, replDestIdxKey, id.Src.storageReplID()), // storage level
	}
	_, err = pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return fmt.Errorf("%w: unable to check if src already used as dest", err)
	}
	for _, res := range boolRes {
		if res.Err() != nil {
			continue
		}
		if res.Val() {
			return fmt.Errorf("%w: unable to create replication policy: src %s is already used as dest", dom.ErrInvalidArg, id.Src.String())
		}
	}

	// check if dst is already used as src in different policy -> circular or cascading replication is not allowed
	pipe = s.client.Pipeline()
	floatRes := []*redis.FloatCmd{
		pipe.ZScore(ctx, replSrcIdxKey, id.Dest.bucketReplID()),  // bucket level
		pipe.ZScore(ctx, replSrcIdxKey, id.Dest.accountReplID()), // account level
		pipe.ZScore(ctx, replSrcIdxKey, id.Dest.storageReplID()), // storage level
	}
	_, err = pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return fmt.Errorf("%w: unable to check if dest already used as src", err)
	}
	for _, res := range floatRes {
		if res.Err() != nil {
			continue
		}
		if res.Val() > 0 {
			return fmt.Errorf("%w: unable to create replication policy: dest %s is already used as src", dom.ErrInvalidArg, id.Dest.String())
		}
	}

	// check if dst is already used as dst in different policy -> cannot merge multiple buckets into one
	pipe = s.client.Pipeline()
	boolRes = []*redis.BoolCmd{
		pipe.SIsMember(ctx, replDestIdxKey, id.Dest.bucketReplID()),  // bucket level
		pipe.SIsMember(ctx, replDestIdxKey, id.Dest.accountReplID()), // account level
		pipe.SIsMember(ctx, replDestIdxKey, id.Dest.storageReplID()), // storage level
	}
	_, err = pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return fmt.Errorf("%w: unable to check if dest already used in diffrent replication", err)
	}
	for _, res := range boolRes {
		if res.Err() != nil {
			continue
		}
		if res.Val() {
			return fmt.Errorf("%w: unable to create replication policy: dest %s is already used as dest", dom.ErrInvalidArg, id.Dest.String())
		}
	}

	// validation done. create policy.

	// 1. block routing requests to the destination bucket if it replicates to the same storage
	if id.Src.Storage == id.Dest.Storage {
		err = s.addRoutingBlockPolicy(ctx, id.Dest.BucketID)
		if err != nil {
			return fmt.Errorf("%w: unable to block routing to dest", err)
		}
		// rollback in case of further errors
		defer func() {
			if err != nil {
				rollbackErr := s.DeleteRoutingPolicy(context.Background(), id.Dest.BucketID)
				if rollbackErr != nil {
					zerolog.Ctx(ctx).Err(rollbackErr).Msgf("unable to rollback routing block for %s", id.Dest.BucketID.String())
				}
			}
		}()
	}
	// 2. add policy if not exists
	var added int64
	added, err = s.client.ZAddNX(ctx, id.Src.bucketReplID(), redis.Z{
		Score:  float64(priority),
		Member: id.Dest.String(),
	}).Result()
	if err != nil {
		return fmt.Errorf("%w: unable to add replication", err)
	}
	if added == 0 {
		return fmt.Errorf("%w: dest %s already exists", dom.ErrAlreadyExists, id.Dest.String())
	}
	// rollback in case of further errors
	defer func() {
		if err != nil {
			rollbackErr := s.client.ZRem(context.Background(), id.Src.bucketReplID(), id.Dest.String()).Err()
			if rollbackErr != nil {
				zerolog.Ctx(ctx).Err(rollbackErr).Msgf("unable to rollback replication policy for %s-%s", id.Src.bucketReplID(), id.Dest.String())
			}
		}
	}()

	// 3. update indexes
	added, err = s.client.SAdd(ctx, replDestIdxKey, id.Dest.bucketReplID()).Result()
	if err != nil {
		return fmt.Errorf("%w: unable to update replication dest index", err)
	}
	if added == 0 {
		return fmt.Errorf("%w: dest index %s already exists", dom.ErrAlreadyExists, id.Dest.bucketReplID())
	}
	defer func() {
		if err != nil {
			rollbackErr := s.client.SRem(context.Background(), replDestIdxKey, id.Dest.bucketReplID()).Err()
			if rollbackErr != nil {
				zerolog.Ctx(ctx).Err(rollbackErr).Msgf("unable to rollback replicaiton dest index %s", id.Dest.bucketReplID())
			}
		}
	}()

	err = s.client.ZIncrBy(ctx, replSrcIdxKey, 1.0, id.Src.bucketReplID()).Err()
	if err != nil {
		return fmt.Errorf("%w: unable to update replication src index", err)
	}

	return nil
}

func (s *policySvc2) DeleteReplicationPolicy(ctx context.Context, id ReplID) error {
	pipe := s.client.Pipeline()
	// delete dest routing block if needed
	if id.Src.Storage == id.Dest.Storage {
		_ = pipe.Del(ctx, id.Dest.bucketRoutingPolicyID())
		_ = pipe.SRem(ctx, routingBlockSetKey, id.Dest.String())
	}
	// delete indexes
	_ = pipe.SRem(ctx, replDestIdxKey, id.Dest.bucketReplID()).Err()
	// delete policy
	_ = pipe.ZRem(context.Background(), id.Src.bucketReplID(), id.Dest.String()).Err()

	res, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		if err == redis.Nil {
			return dom.ErrNotFound
		}
		return fmt.Errorf("%w: unable to delete replication", err)
	}
	for _, r := range res {
		err = r.Err()
		if err == nil {
			continue
		}
		if err == redis.Nil {
			return dom.ErrNotFound
		}
		return fmt.Errorf("%w: unable to delete replication", err)
	}
	// decrease src index separately because there anre no INC NX inredis
	// todo: rewrite whole DeleteReplicationPolicy() into single lua script to be atomic.
	err = luaZIncrByEx.Run(ctx, s.client, []string{replSrcIdxKey}, id.Src.bucketReplID(), -1.0).Err()
	if err != nil {
		return fmt.Errorf("%w: unable to delete repl src index for %s", err, id.Src.bucketReplID())
	}
	return nil
}

func (s *policySvc2) DoReplicationSwitch(ctx context.Context, id ReplID) error {
	panic("unimplemented")
}

func (s *policySvc2) GetReplicationPolicyInfo(ctx context.Context, id ReplID) (ReplicationPolicyStatus, error) {
	panic("unimplemented")
}

func (s *policySvc2) GetReplicationSwitch(ctx context.Context, src StorageBucketID) (ReplicationSwitch, error) {
	panic("unimplemented")
}

func (s *policySvc2) IncReplEvents(ctx context.Context, id ReplID, eventTime time.Time) error {
	panic("unimplemented")
}

func (s *policySvc2) AddReplicationInfo(ctx context.Context, id ReplID, agentURL *string) error {
	panic("unimplemented")
}

func (s *policySvc2) IncReplEventsDone(ctx context.Context, id ReplID, eventTime time.Time) error {
	panic("unimplemented")
}

func (s *policySvc2) IncReplInitObjDone(ctx context.Context, id ReplID, bytes int64, eventTime time.Time) error {
	panic("unimplemented")
}

func (s *policySvc2) IncReplInitObjListed(ctx context.Context, id ReplID, bytes int64, eventTime time.Time) error {
	panic("unimplemented")
}

func (s *policySvc2) IsReplicationPolicyExists(ctx context.Context, id ReplID) (bool, error) {
	panic("unimplemented")
}

func (s *policySvc2) IsReplicationPolicyPaused(ctx context.Context, id ReplID) (bool, error) {
	panic("unimplemented")
}

func (s *policySvc2) IsReplicationSwitchInProgress(ctx context.Context, src StorageBucketID) (bool, error) {
	panic("unimplemented")
}

func (s *policySvc2) ListReplicationPolicyInfo(ctx context.Context) ([]ReplicationPolicyStatusExtended, error) {
	panic("unimplemented")
}

func (s *policySvc2) ObjListStarted(ctx context.Context, id ReplID) error {
	panic("unimplemented")
}

func (s *policySvc2) PauseReplication(ctx context.Context, id ReplID) error {
	panic("unimplemented")
}

func (s *policySvc2) ReplicationSwitchDone(ctx context.Context, src StorageBucketID) error {
	panic("unimplemented")
}

func (s *policySvc2) ResumeReplication(ctx context.Context, id ReplID) error {
	panic("unimplemented")
}
