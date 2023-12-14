package test

import (
	"bytes"
	mclient "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
	"io"
	"math/rand"
	"testing"
	"time"
)

func TestApi_Object_CRUD(t *testing.T) {
	t.Parallel()
	bucket := "object-crud"
	r := require.New(t)

	err := proxyClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{Region: "us-east"})
	r.NoError(err)
	t.Cleanup(func() {
		cleanup(t, false, bucket)
	})
	ok, err := proxyClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.True(ok)

	r.Eventually(func() bool {
		ok, err = mainClient.BucketExists(tstCtx, bucket)
		if err != nil || !ok {
			return false
		}
		ok, err = f1Client.BucketExists(tstCtx, bucket)
		if err != nil || !ok {
			return false
		}
		ok, err = f2Client.BucketExists(tstCtx, bucket)
		if err != nil || !ok {
			return false
		}
		return true
	}, time.Second*3, time.Millisecond*100)

	objName := "obj-crud"
	_, err = mainClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)
	_, err = f1Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)
	_, err = f2Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)
	_, err = proxyClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)

	source := bytes.Repeat([]byte("3"), rand.Intn(1<<20)+32*1024)

	putInfo, err := proxyClient.PutObject(tstCtx, bucket, objName, bytes.NewReader(source), int64(len(source)), mclient.PutObjectOptions{
		ContentType: "binary/octet-stream", DisableContentSha256: true,
	})
	r.NoError(err)
	r.EqualValues(objName, putInfo.Key)
	r.EqualValues(bucket, putInfo.Bucket)

	obj, err := proxyClient.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
	r.NoError(err)

	objBytes, err := io.ReadAll(obj)
	r.NoError(err)
	r.EqualValues(source, objBytes)

	_, err = proxyClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.NoError(err)

	r.Eventually(func() bool {
		_, err = mainClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		_, err = f1Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		_, err = f2Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		return true
	}, time.Second*3, time.Millisecond*100)

	obj, err = mainClient.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
	r.NoError(err)
	objBytes, err = io.ReadAll(obj)
	r.NoError(err)
	r.EqualValues(source, objBytes)

	obj, err = f1Client.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
	r.NoError(err)
	objBytes, err = io.ReadAll(obj)
	r.NoError(err)
	r.EqualValues(source, objBytes)

	obj, err = f2Client.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
	r.NoError(err)
	objBytes, err = io.ReadAll(obj)
	r.NoError(err)
	r.EqualValues(source, objBytes)

	updated := bytes.Repeat([]byte("3"), rand.Intn(1<<20)+32*1024)
	r.NotEqualValues(source, updated)

	putInfo, err = proxyClient.PutObject(tstCtx, bucket, objName, bytes.NewReader(updated), int64(len(updated)), mclient.PutObjectOptions{
		ContentType: "binary/octet-stream", DisableContentSha256: true,
	})
	r.NoError(err)
	r.EqualValues(objName, putInfo.Key)
	r.EqualValues(bucket, putInfo.Bucket)

	obj, err = proxyClient.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
	r.NoError(err)

	objBytes, err = io.ReadAll(obj)
	r.NoError(err)
	r.EqualValues(updated, objBytes)

	_, err = proxyClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.NoError(err)

	r.Eventually(func() bool {
		obj, err = mainClient.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
		if err != nil {
			return false
		}
		objBytes, err = io.ReadAll(obj)
		if err != nil {
			return false
		}
		if !bytes.Equal(updated, objBytes) {
			return false
		}

		obj, err = f1Client.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
		if err != nil {
			return false
		}
		objBytes, err = io.ReadAll(obj)
		if err != nil {
			return false
		}
		if !bytes.Equal(updated, objBytes) {
			return false
		}

		obj, err = f2Client.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
		if err != nil {
			return false
		}
		objBytes, err = io.ReadAll(obj)
		if err != nil {
			return false
		}
		if !bytes.Equal(updated, objBytes) {
			return false
		}

		return true
	}, time.Second*3, time.Millisecond*100)

	err = proxyClient.RemoveObject(tstCtx, bucket, objName, mclient.RemoveObjectOptions{})
	r.NoError(err)

	_, err = proxyClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)

	r.Eventually(func() bool {
		_, err = mainClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err == nil {
			return false
		}
		_, err = f1Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err == nil {
			return false
		}
		_, err = f2Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err == nil {
			return false
		}
		return true
	}, time.Second*3, time.Millisecond*100)

}
