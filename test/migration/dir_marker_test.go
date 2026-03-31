package migration

import (
	"bytes"
	"io"
	"testing"

	mclient "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/test/app"
)

func TestApi_Migrate_DirectoryMarkerObject(t *testing.T) {
	e := app.SetupEmbedded(t, workerConf, proxyConf)
	tstCtx := t.Context()
	r := require.New(t)

	bucket := "migrate-dir-marker"
	emptyMarker := "empty/"
	nonEmptyMarker := "folder/"
	nestedObj := getTestObj("folder/file.txt", bucket)

	err := e.MainClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{})
	r.NoError(err)

	_, err = e.MainClient.PutObject(tstCtx, bucket, emptyMarker, bytes.NewReader(nil), 0, mclient.PutObjectOptions{
		ContentType: "application/x-directory", DisableContentSha256: true,
	})
	r.NoError(err)

	_, err = e.MainClient.PutObject(tstCtx, bucket, nonEmptyMarker, bytes.NewReader(nil), 0, mclient.PutObjectOptions{
		ContentType: "application/x-directory", DisableContentSha256: true,
	})
	r.NoError(err)

	_, err = e.MainClient.PutObject(tstCtx, nestedObj.bucket, nestedObj.name, bytes.NewReader(nestedObj.data), int64(len(nestedObj.data)), mclient.PutObjectOptions{
		ContentType: "binary/octet-stream",
	})
	r.NoError(err)

	id := &pb.ReplicationID{
		FromBucket:  &bucket,
		ToBucket:    &bucket,
		FromStorage: "main",
		ToStorage:   "f1",
		User:        user,
	}

	_, err = e.PolicyClient.AddReplication(tstCtx, &pb.AddReplicationRequest{Id: id})
	r.NoError(err)

	r.Eventually(func() bool {
		_, err := e.F1Client.StatObject(tstCtx, bucket, emptyMarker, mclient.StatObjectOptions{})
		return err == nil
	}, e.WaitLong, e.RetryLong)

	r.Eventually(func() bool {
		_, err := e.F1Client.StatObject(tstCtx, bucket, nonEmptyMarker, mclient.StatObjectOptions{})
		return err == nil
	}, e.WaitLong, e.RetryLong)

	r.Eventually(func() bool {
		obj, err := e.F1Client.GetObject(tstCtx, bucket, nonEmptyMarker, mclient.GetObjectOptions{})
		if err != nil {
			return false
		}
		defer obj.Close()

		body, err := io.ReadAll(obj)
		return err == nil && len(body) == 0
	}, e.WaitLong, e.RetryLong)

	r.Eventually(func() bool {
		obj, err := e.F1Client.GetObject(tstCtx, nestedObj.bucket, nestedObj.name, mclient.GetObjectOptions{})
		if err != nil {
			return false
		}
		defer obj.Close()

		body, err := io.ReadAll(obj)
		return err == nil && bytes.Equal(body, nestedObj.data)
	}, e.WaitLong, e.RetryLong)

	diff := replicationDiff(t, e, id)
	r.True(diff.IsMatch)
	r.Empty(diff.Differ)
	r.Empty(diff.MissFrom)
	r.Empty(diff.MissTo)
}

func TestApi_Migrate_DirectoryMarkerObjectVersioned(t *testing.T) {
	e := app.SetupEmbedded(t, workerConf, proxyConf)
	tstCtx := t.Context()
	r := require.New(t)

	bucket := "migrate-dir-marker-versioned"
	marker := "folder/"
	nestedObj := getTestObj("folder/file.txt", bucket)

	err := e.MainClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{})
	r.NoError(err)
	err = e.MainClient.EnableVersioning(tstCtx, bucket)
	r.NoError(err)

	_, err = e.MainClient.PutObject(tstCtx, bucket, marker, bytes.NewReader(nil), 0, mclient.PutObjectOptions{
		ContentType: "application/x-directory", DisableContentSha256: true,
	})
	r.NoError(err)

	_, err = e.MainClient.PutObject(tstCtx, nestedObj.bucket, nestedObj.name, bytes.NewReader(nestedObj.data), int64(len(nestedObj.data)), mclient.PutObjectOptions{
		ContentType: "binary/octet-stream", DisableContentSha256: true,
	})
	r.NoError(err)

	id := &pb.ReplicationID{
		FromBucket:  &bucket,
		ToBucket:    &bucket,
		FromStorage: "main",
		ToStorage:   "f1",
		User:        user,
	}

	_, err = e.PolicyClient.AddReplication(tstCtx, &pb.AddReplicationRequest{Id: id})
	r.NoError(err)

	r.Eventually(func() bool {
		_, err := e.F1Client.StatObject(tstCtx, bucket, marker, mclient.StatObjectOptions{})
		return err == nil
	}, e.WaitLong, e.RetryLong)

	r.Eventually(func() bool {
		obj, err := e.F1Client.GetObject(tstCtx, bucket, marker, mclient.GetObjectOptions{})
		if err != nil {
			return false
		}
		defer obj.Close()

		body, err := io.ReadAll(obj)
		return err == nil && len(body) == 0
	}, e.WaitLong, e.RetryLong)

	r.Eventually(func() bool {
		obj, err := e.F1Client.GetObject(tstCtx, nestedObj.bucket, nestedObj.name, mclient.GetObjectOptions{})
		if err != nil {
			return false
		}
		defer obj.Close()

		body, err := io.ReadAll(obj)
		return err == nil && bytes.Equal(body, nestedObj.data)
	}, e.WaitLong, e.RetryLong)

	diff := replicationDiff(t, e, id)
	r.True(diff.IsMatch)
	r.Empty(diff.Differ)
	r.Empty(diff.MissFrom)
	r.Empty(diff.MissTo)
}
