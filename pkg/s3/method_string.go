// Code generated by "stringer -type Method"; DO NOT EDIT.

package s3

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[UndefinedMethod-0]
	_ = x[CreateBucket-1]
	_ = x[DeleteBucket-2]
	_ = x[HeadBucket-3]
	_ = x[ListBuckets-4]
	_ = x[GetBucketLocation-5]
	_ = x[GetBucketTagging-6]
	_ = x[PutBucketTagging-7]
	_ = x[DeleteBucketTagging-8]
	_ = x[GetBucketLifecycle-9]
	_ = x[DeleteBucketLifecycle-10]
	_ = x[PutBucketLifecycle-11]
	_ = x[GetBucketPolicy-12]
	_ = x[PutBucketPolicy-13]
	_ = x[DeleteBucketPolicy-14]
	_ = x[GetBucketPolicyStatus-15]
	_ = x[GetBucketAcl-16]
	_ = x[PutBucketAcl-17]
	_ = x[GetBucketVersioning-18]
	_ = x[PutBucketVersioning-19]
	_ = x[GetBucketWebsite-20]
	_ = x[PutBucketWebsite-21]
	_ = x[DeleteBucketWebsite-22]
	_ = x[GetBucketReplication-23]
	_ = x[PutBucketReplication-24]
	_ = x[DeleteBucketReplication-25]
	_ = x[GetBucketNotification-26]
	_ = x[DeleteBucketNotification-27]
	_ = x[PutBucketNotification-28]
	_ = x[GetBucketEncryption-29]
	_ = x[PutBucketEncryption-30]
	_ = x[DeleteBucketEncryption-31]
	_ = x[GetBucketRequestPayment-32]
	_ = x[PutBucketRequestPayment-33]
	_ = x[GetBucketMetricsConfiguration-34]
	_ = x[ListBucketMetricsConfiguration-35]
	_ = x[PutBucketMetricsConfiguration-36]
	_ = x[DeleteBucketMetricsConfiguration-37]
	_ = x[GetBucketAnalyticsConfiguration-38]
	_ = x[ListBucketAnalyticsConfiguration-39]
	_ = x[PutBucketAnalyticsConfiguration-40]
	_ = x[DeleteBucketAnalyticsConfiguration-41]
	_ = x[GetBucketIntelligentTieringConfiguration-42]
	_ = x[ListBucketIntelligentTieringConfiguration-43]
	_ = x[PutBucketIntelligentTieringConfiguration-44]
	_ = x[DeleteBucketIntelligentTieringConfiguration-45]
	_ = x[GetBucketInventoryConfiguration-46]
	_ = x[ListBucketInventoryConfiguration-47]
	_ = x[PutBucketInventoryConfiguration-48]
	_ = x[DeleteBucketInventoryConfiguration-49]
	_ = x[GetBucketAccelerateConfiguration-50]
	_ = x[PutBucketAccelerateConfiguration-51]
	_ = x[GetBucketLogging-52]
	_ = x[PutBucketLogging-53]
	_ = x[GetBucketOwnershipControls-54]
	_ = x[PutBucketOwnershipControls-55]
	_ = x[DeleteBucketOwnershipControls-56]
	_ = x[GetBucketCors-57]
	_ = x[PutBucketCors-58]
	_ = x[DeleteBucketCors-59]
	_ = x[GetObject-60]
	_ = x[HeadObject-61]
	_ = x[PutObject-62]
	_ = x[DeleteObject-63]
	_ = x[DeleteObjects-64]
	_ = x[ListObjects-65]
	_ = x[ListObjectsV2-66]
	_ = x[GetObjectAttributes-67]
	_ = x[CopyObject-68]
	_ = x[ListObjectVersions-69]
	_ = x[RestoreObject-70]
	_ = x[SelectObjectContent-71]
	_ = x[WriteGetObjectResponse-72]
	_ = x[CreateMultipartUpload-73]
	_ = x[UploadPart-74]
	_ = x[CompleteMultipartUpload-75]
	_ = x[AbortMultipartUpload-76]
	_ = x[ListMultipartUploads-77]
	_ = x[ListParts-78]
	_ = x[UploadPartCopy-79]
	_ = x[GetObjectTagging-80]
	_ = x[PutObjectTagging-81]
	_ = x[DeleteObjectTagging-82]
	_ = x[GetObjectAcl-83]
	_ = x[PutObjectAcl-84]
	_ = x[GetPublicAccessBlock-85]
	_ = x[PutPublicAccessBlock-86]
	_ = x[DeletePublicAccessBlock-87]
	_ = x[GetObjectRetention-88]
	_ = x[PutObjectRetention-89]
	_ = x[GetObjectLegalHold-90]
	_ = x[PutObjectLegalHold-91]
	_ = x[GetObjectLockConfiguration-92]
	_ = x[PutObjectLockConfiguration-93]
	_ = x[GetObjectTorrent-94]
}

const _Method_name = "UndefinedMethodCreateBucketDeleteBucketHeadBucketListBucketsGetBucketLocationGetBucketTaggingPutBucketTaggingDeleteBucketTaggingGetBucketLifecycleDeleteBucketLifecyclePutBucketLifecycleGetBucketPolicyPutBucketPolicyDeleteBucketPolicyGetBucketPolicyStatusGetBucketAclPutBucketAclGetBucketVersioningPutBucketVersioningGetBucketWebsitePutBucketWebsiteDeleteBucketWebsiteGetBucketReplicationPutBucketReplicationDeleteBucketReplicationGetBucketNotificationDeleteBucketNotificationPutBucketNotificationGetBucketEncryptionPutBucketEncryptionDeleteBucketEncryptionGetBucketRequestPaymentPutBucketRequestPaymentGetBucketMetricsConfigurationListBucketMetricsConfigurationPutBucketMetricsConfigurationDeleteBucketMetricsConfigurationGetBucketAnalyticsConfigurationListBucketAnalyticsConfigurationPutBucketAnalyticsConfigurationDeleteBucketAnalyticsConfigurationGetBucketIntelligentTieringConfigurationListBucketIntelligentTieringConfigurationPutBucketIntelligentTieringConfigurationDeleteBucketIntelligentTieringConfigurationGetBucketInventoryConfigurationListBucketInventoryConfigurationPutBucketInventoryConfigurationDeleteBucketInventoryConfigurationGetBucketAccelerateConfigurationPutBucketAccelerateConfigurationGetBucketLoggingPutBucketLoggingGetBucketOwnershipControlsPutBucketOwnershipControlsDeleteBucketOwnershipControlsGetBucketCorsPutBucketCorsDeleteBucketCorsGetObjectHeadObjectPutObjectDeleteObjectDeleteObjectsListObjectsListObjectsV2GetObjectAttributesCopyObjectListObjectVersionsRestoreObjectSelectObjectContentWriteGetObjectResponseCreateMultipartUploadUploadPartCompleteMultipartUploadAbortMultipartUploadListMultipartUploadsListPartsUploadPartCopyGetObjectTaggingPutObjectTaggingDeleteObjectTaggingGetObjectAclPutObjectAclGetPublicAccessBlockPutPublicAccessBlockDeletePublicAccessBlockGetObjectRetentionPutObjectRetentionGetObjectLegalHoldPutObjectLegalHoldGetObjectLockConfigurationPutObjectLockConfigurationGetObjectTorrent"

var _Method_index = [...]uint16{0, 15, 27, 39, 49, 60, 77, 93, 109, 128, 146, 167, 185, 200, 215, 233, 254, 266, 278, 297, 316, 332, 348, 367, 387, 407, 430, 451, 475, 496, 515, 534, 556, 579, 602, 631, 661, 690, 722, 753, 785, 816, 850, 890, 931, 971, 1014, 1045, 1077, 1108, 1142, 1174, 1206, 1222, 1238, 1264, 1290, 1319, 1332, 1345, 1361, 1370, 1380, 1389, 1401, 1414, 1425, 1438, 1457, 1467, 1485, 1498, 1517, 1539, 1560, 1570, 1593, 1613, 1633, 1642, 1656, 1672, 1688, 1707, 1719, 1731, 1751, 1771, 1794, 1812, 1830, 1848, 1866, 1892, 1918, 1934}

func (i Method) String() string {
	if i >= Method(len(_Method_index)-1) {
		return "Method(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _Method_name[_Method_index[i]:_Method_index[i+1]]
}
