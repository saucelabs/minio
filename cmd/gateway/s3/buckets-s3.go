/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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

package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"

	miniogo "github.com/minio/minio-go"
	"github.com/minio/minio-go/pkg/s3utils"
	"github.com/minio/minio/cmd/logger"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/policy"
)

type buckets interface {
	GetBucketInfo(ctx context.Context, bucket string) (bi minio.BucketInfo, e error)
	ListBuckets(ctx context.Context) ([]minio.BucketInfo, error)
	MakeBucketWithLocation(ctx context.Context, bucket, location string) error
	DeleteBucket(ctx context.Context, bucket string) error
}

// default bucket manager relies on s3 for bucket list
type s3Buckets struct {
	Client *miniogo.Core
}

// GetBucketInfo gets bucket metadata..
func (l *s3Buckets) GetBucketInfo(ctx context.Context, bucket string) (bi minio.BucketInfo, e error) {
	buckets, err := l.Client.ListBuckets()
	if err != nil {
		logger.LogIf(ctx, err)
		return bi, minio.ErrorRespToObjectError(err, bucket)
	}

	for _, bi := range buckets {
		if bi.Name != bucket {
			continue
		}

		return minio.BucketInfo{
			Name:    bi.Name,
			Created: bi.CreationDate,
		}, nil
	}

	return bi, minio.BucketNotFound{Bucket: bucket}
}

// ListBuckets lists all S3 buckets
func (l *s3Buckets) ListBuckets(ctx context.Context) ([]minio.BucketInfo, error) {
	buckets, err := l.Client.ListBuckets()
	if err != nil {
		logger.LogIf(ctx, err)
		return nil, minio.ErrorRespToObjectError(err)
	}

	b := make([]minio.BucketInfo, len(buckets))
	for i, bi := range buckets {
		b[i] = minio.BucketInfo{
			Name:    bi.Name,
			Created: bi.CreationDate,
		}
	}

	return b, err
}

// MakeBucket creates a new container on S3 backend.
func (l *s3Buckets) MakeBucketWithLocation(ctx context.Context, bucket, location string) error {
	// Verify if bucket name is valid.
	// We are using a separate helper function here to validate bucket
	// names instead of IsValidBucketName() because there is a possibility
	// that certains users might have buckets which are non-DNS compliant
	// in us-east-1 and we might severely restrict them by not allowing
	// access to these buckets.
	// Ref - http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
	if s3utils.CheckValidBucketName(bucket) != nil {
		logger.LogIf(ctx, minio.BucketNameInvalid{Bucket: bucket})
		return minio.BucketNameInvalid{Bucket: bucket}
	}

	err := l.Client.MakeBucket(bucket, location)
	if err != nil {
		logger.LogIf(ctx, err)
		return minio.ErrorRespToObjectError(err, bucket)
	}
	return err
}

// DeleteBucket deletes a bucket on S3
func (l *s3Buckets) DeleteBucket(ctx context.Context, bucket string) error {
	err := l.Client.RemoveBucket(bucket)
	if err != nil {
		logger.LogIf(ctx, err)
		return minio.ErrorRespToObjectError(err, bucket)
	}
	return nil
}

// SetBucketPolicy sets policy on bucket
func (l *s3Buckets) SetBucketPolicy(ctx context.Context, bucket string, bucketPolicy *policy.Policy) error {
	data, err := json.Marshal(bucketPolicy)
	if err != nil {
		// This should not happen.
		logger.LogIf(ctx, err)
		return minio.ErrorRespToObjectError(err, bucket)
	}

	if err := l.Client.SetBucketPolicy(bucket, string(data)); err != nil {
		logger.LogIf(ctx, err)
		return minio.ErrorRespToObjectError(err, bucket)
	}

	return nil
}

// GetBucketPolicy will get policy on bucket
func (l *s3Buckets) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	data, err := l.Client.GetBucketPolicy(bucket)
	if err != nil {
		logger.LogIf(ctx, err)
		return nil, minio.ErrorRespToObjectError(err, bucket)
	}

	bucketPolicy, err := policy.ParseConfig(strings.NewReader(data), bucket)
	return bucketPolicy, minio.ErrorRespToObjectError(err, bucket)
}

// DeleteBucketPolicy deletes all policies on bucket
func (l *s3Buckets) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	if err := l.Client.SetBucketPolicy(bucket, ""); err != nil {
		logger.LogIf(ctx, err)
		return minio.ErrorRespToObjectError(err, bucket, "")
	}
	return nil
}

// Alternate bucket manager using a master bucket to track the list of
// buckets and creation dates.
//
// Why is it needed:
// The only method to provided by the S3 api to retrieve the bucket list
// and bucket creation dates is ListAllBuckets. This is inneficient when
// the S3 root contains a large number of buckets, and there are security
// issues since ListAllBuckets permission must be given to the minio account.
// This implemention goes round these limitations, and let you assign
// discrete buckets to be managed by Minio.
//
// Caveat:
// The bucket creation date cannot be retrieved from Minio, so we use a
// value stored in the master bucket instead.
type minioS3Buckets struct {
	*s3Buckets
	masterBucket string
	Client *miniogo.Core
}

// GetBucketInfo gets bucket metadata..
func (l *minioS3Buckets) GetBucketInfo(ctx context.Context, bucket string) (bi minio.BucketInfo, e error) {
	opts := miniogo.StatObjectOptions{}
	oi, err := l.Client.StatObject(l.masterBucket, bucket, opts)
	if err != nil {
		err = minio.ErrorRespToObjectError(err)
		if _, ok := err.(minio.ObjectNotFound); ok {
			return bi, minio.BucketNotFound{Bucket: bucket}
		}
		logger.LogIf(ctx, err)
		return bi, err
	}
	bi = minio.BucketInfo{
		Name: bucket,
		Created: oi.LastModified,
	}
	return bi, nil
}

//// ListBuckets lists all S3 buckets
func (l *minioS3Buckets) ListBuckets(ctx context.Context) ([]minio.BucketInfo, error) {
	// retrieve list from master bucket
	result, err := l.Client.ListObjects(l.masterBucket, "", "", "", 1000)
	ois := make([]minio.ObjectInfo, len(result.Contents))
	for i, oi := range result.Contents {
		ois[i] = minio.FromMinioClientObjectInfo(l.masterBucket, oi)
	}


	if err != nil {
		logger.LogIf(ctx, err)
		return nil, minio.ErrorRespToObjectError(err)
	}

	b := make([]minio.BucketInfo, len(ois))
	for i, oi := range ois {
		b[i] = minio.BucketInfo{
			Name:    oi.Name,
			Created: oi.ModTime,
		}
	}

	return b, err
}

// MakeBucket creates a new container on S3 backend.
func (l *minioS3Buckets) MakeBucketWithLocation(ctx context.Context, bucket, location string) error {
	// check that bucket does not exist
	_, err := l.Client.StatObject(l.masterBucket, bucket, miniogo.StatObjectOptions{})
	if err == nil {
			return minio.BucketExists{Bucket: bucket}
	}
	err = minio.ErrorRespToObjectError(err)
	if _, ok := err.(minio.BucketNotFound); !ok {
		return err
	}

	// create S3 bucket
	err = l.s3Buckets.MakeBucketWithLocation(ctx, bucket, location)
	if err != nil {
	_, ok := err.(minio.BucketExists)
	if !ok {
		_, ok = err.(minio.BucketAlreadyOwnedByYou)
	}
	// if bucket already exists we ignore the error and add the missing record
	if !ok {
		return err
	}
}

	// add bucket record
	data, err := hash.NewReader(bytes.NewBufferString(""), int64(len("")), "", "")
		_, err = l.Client.PutObject(l.masterBucket, bucket, data, data.Size(), data.MD5Base64String(), data.SHA256HexString(), nil)
	if err != nil {
		return err
	}
	return nil
}

// DeleteBucket deletes a bucket on S3
func (l *minioS3Buckets) DeleteBucket(ctx context.Context, bucket string) error {
	err := l.s3Buckets.DeleteBucket(ctx, bucket)
	if err != nil {
		return err
	}
	// delete bucket from master bucket
	err = l.Client.RemoveObject(l.masterBucket, bucket)
	if err != nil {
		return minio.ErrorRespToObjectError(err)
	}
	return nil
}
