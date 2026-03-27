package handler

import (
	"testing"

	mclient "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/features"
	"github.com/clyso/chorus/pkg/tasks"
)

func TestTasksForListedObject_DirectoryMarker_NonVersioned_FeatureEnabled(t *testing.T) {
	features.Set(&features.Config{DirectoryMarkers: true})

	object := mclient.ObjectInfo{Key: "photos/", Size: 0, ETag: "etag-dir", ContentType: "application/x-directory"}
	p := tasks.MigrateBucketListObjectsPayload{Bucket: "bkt", Versioned: false}
	id := entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{User: "u", FromStorage: "main", ToStorage: "f1", FromBucket: "bkt", ToBucket: "bkt"})

	enqueued, isDir := tasksForListedObject(t.Context(), p, object, id)

	r := require.New(t)
	r.True(isDir)
	r.Len(enqueued, 2)

	copyTask, ok := enqueued[0].(tasks.MigrateObjCopyPayload)
	r.True(ok)
	r.Equal("bkt", copyTask.Bucket)
	r.Equal("photos/", copyTask.Obj.Name)
	r.EqualValues(0, copyTask.Obj.Size)

	subTask, ok := enqueued[1].(tasks.MigrateBucketListObjectsPayload)
	r.True(ok)
	r.Equal("photos/", subTask.Prefix)
}

func TestTasksForListedObject_DirectoryMarker_NonVersioned_FeatureDisabled(t *testing.T) {
	features.Set(&features.Config{DirectoryMarkers: false})

	object := mclient.ObjectInfo{Key: "photos/", Size: 0}
	p := tasks.MigrateBucketListObjectsPayload{Bucket: "bkt", Versioned: false}
	id := entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{User: "u", FromStorage: "main", ToStorage: "f1", FromBucket: "bkt", ToBucket: "bkt"})

	enqueued, isDir := tasksForListedObject(t.Context(), p, object, id)

	r := require.New(t)
	r.True(isDir)
	r.Len(enqueued, 1)

	subTask, ok := enqueued[0].(tasks.MigrateBucketListObjectsPayload)
	r.True(ok)
	r.Equal("photos/", subTask.Prefix)
}

func TestTasksForListedObject_DirectoryMarker_Versioned_FeatureEnabled(t *testing.T) {
	features.Set(&features.Config{DirectoryMarkers: true})

	object := mclient.ObjectInfo{Key: "photos/", Size: 0}
	p := tasks.MigrateBucketListObjectsPayload{Bucket: "bkt", Versioned: true}
	id := entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{User: "u", FromStorage: "main", ToStorage: "f1", FromBucket: "bkt", ToBucket: "bkt"})

	enqueued, isDir := tasksForListedObject(t.Context(), p, object, id)

	r := require.New(t)
	r.True(isDir)
	r.Len(enqueued, 2)

	listVersionsTask, ok := enqueued[0].(tasks.ListObjectVersionsPayload)
	r.True(ok)
	r.Equal("bkt", listVersionsTask.Bucket)
	r.Equal("photos/", listVersionsTask.Prefix)

	subTask, ok := enqueued[1].(tasks.MigrateBucketListObjectsPayload)
	r.True(ok)
	r.Equal("photos/", subTask.Prefix)
}

func TestTasksForListedObject_RegularObject_NonVersioned(t *testing.T) {
	features.Set(&features.Config{DirectoryMarkers: true})

	object := mclient.ObjectInfo{Key: "photos/file.txt", Size: 12, ETag: "etag-file", ContentType: "text/plain"}
	p := tasks.MigrateBucketListObjectsPayload{Bucket: "bkt", Versioned: false}
	id := entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{User: "u", FromStorage: "main", ToStorage: "f1", FromBucket: "bkt", ToBucket: "bkt"})

	enqueued, isDir := tasksForListedObject(t.Context(), p, object, id)

	r := require.New(t)
	r.False(isDir)
	r.Len(enqueued, 1)

	copyTask, ok := enqueued[0].(tasks.MigrateObjCopyPayload)
	r.True(ok)
	r.Equal("photos/file.txt", copyTask.Obj.Name)
	r.EqualValues(12, copyTask.Obj.Size)
}
