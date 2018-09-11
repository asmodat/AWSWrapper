using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using AWSWrapper.Extensions;
using AsmodatStandard.Extensions;
using AsmodatStandard.Extensions.Collections;
using System.IO;
using Amazon.Runtime;
using Amazon.SecurityToken.Model;

namespace AWSWrapper.S3
{
    public partial class S3Helper
    {
        public enum Permissions
        {
            None = 0,
            AbortMultipartUpload = 1,
            DeleteObject = 1 << 1,
            DeleteObjectTagging = 1 << 2,
            DeleteObjectVersion = 1 << 3,
            DeleteObjectVersionTagging = 1 << 4,
            GetObject = 1 << 5,
            GetObjectAcl = 1 << 6,
            GetObjectTagging = 1 << 7,
            GetObjectTorrent = 1 << 8,
            GetObjectVersion = 1 << 9,
            GetObjectVersionAcl = 1 << 10,
            GetObjectVersionTagging = 1 << 11,
            GetObjectVersionTorrent = 1 << 12,
            ListMultipartUploadParts = 1 << 13,
            PutObject = 1 << 14,
            PutObjectAcl = 1 << 15,
            PutObjectTagging = 1 << 16,
            PutObjectVersionAcl = 1 << 17,
            PutObjectVersionTagging = 1 << 18,
            RestoreObject = 1 << 19,
            Read = GetObject | GetObjectAcl | GetObjectTagging | GetObjectTorrent | GetObjectVersion | GetObjectVersionTagging | GetObjectVersionTorrent | ListMultipartUploadParts,
            Write = AbortMultipartUpload | PutObject | PutObjectAcl | PutObjectTagging | PutObjectVersionAcl | PutObjectVersionTagging | RestoreObject,
            Delete = DeleteObject | DeleteObjectTagging | DeleteObjectVersion | DeleteObjectVersionTagging,
            All = Read | Write | Delete
        }

        public readonly int MaxSinglePartSize = 5 * 1024 * 1025;
        public readonly int DefaultPartSize = 5 * 1024 * 1025;
        internal readonly int _maxDegreeOfParalelism;
        internal readonly AmazonS3Client _S3Client;
        internal readonly Credentials _credentials;

        public S3Helper(Credentials credentials = null, int maxDegreeOfParalelism = 2)
        {
            _maxDegreeOfParalelism = maxDegreeOfParalelism;
            _credentials = credentials;

            if (credentials != null)
                _S3Client = new AmazonS3Client(credentials);
            else
                _S3Client = new AmazonS3Client();
        }

        public Task<GetObjectMetadataResponse> GetObjectMetadata(
            string bucketName,
            string key = null,
            CancellationToken cancellationToken = default(CancellationToken))
            => _S3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest()
            {
                BucketName = bucketName,
                Key = key,
            }, cancellationToken).EnsureSuccessAsync();

        public async Task<bool> DeleteObjectAsync(
            string bucketName,
            string key = null,
            string versionId = null,
            bool throwOnFailure = true,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var request = new DeleteObjectRequest()
            {
                BucketName = bucketName,
                Key = key,
                VersionId = versionId
            };

            var result = await _S3Client.DeleteObjectAsync(request, cancellationToken);

            if (result.HttpStatusCode != System.Net.HttpStatusCode.OK)
            {
                var exists = await this.ObjectExistsAsync(bucketName: bucketName, key: key);

                if (exists && throwOnFailure)
                    throw new Exception($"Object '{key}', still exists in the bucket '{bucketName}'/");

                return !exists;
            }
            else
                return true;
        }

        public Task<GetObjectResponse> GetObjectAsync(
            string bucketName,
            string key = null,
            string versionId = null,
            string keyId = null,
            string eTag = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var request = new GetObjectRequest()
            {
                BucketName = bucketName,
                Key = key,
                VersionId = versionId,
                EtagToMatch = eTag,
            };

            return _S3Client.GetObjectAsync(request, cancellationToken).EnsureSuccessAsync();
        }

        public Task<DeleteObjectsResponse> DeleteObjectsAsync(
            string bucketName,
            IEnumerable<KeyVersion> objects,
            CancellationToken cancellationToken = default(CancellationToken))
            => _S3Client.DeleteObjectsAsync(
                new DeleteObjectsRequest() {
                    BucketName = bucketName,
                    Quiet = false,
                    Objects = objects.ToList(),
                },
                cancellationToken).EnsureSuccessAsync();

        public Task<InitiateMultipartUploadResponse> InitiateMultipartUploadAsync(
            string bucketName,
            string key,
            string contentType,
            string keyId = null,
            CancellationToken cancellationToken = default(CancellationToken))
            => _S3Client.InitiateMultipartUploadAsync(
                new InitiateMultipartUploadRequest() {
                    BucketName = bucketName,
                    Key = key,
                    ContentType = contentType,
                    ServerSideEncryptionKeyManagementServiceKeyId = keyId
                }, 
                cancellationToken).EnsureSuccessAsync();

        public Task<PutObjectResponse> PutObjectAsync(
            string bucketName,
            string key,
            Stream inputStream,
            string keyId = null,
            Action<object, StreamTransferProgressArgs> progress = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            if (inputStream.Length > MaxSinglePartSize)
                throw new ArgumentException($"Part size in singlepart upload can't exceed {MaxSinglePartSize} B, but was {inputStream.Length} B, bucket: {bucketName}, key: {key}.");

            var request = new PutObjectRequest()
            {
                BucketName = bucketName,
                Key = key,
                InputStream = inputStream,
                ServerSideEncryptionKeyManagementServiceKeyId = keyId,
            };

            if (progress != null)
                request.StreamTransferProgress += new EventHandler<StreamTransferProgressArgs>(progress);

            return _S3Client.PutObjectAsync(request, cancellationToken).EnsureSuccessAsync();
        }

        public Task<UploadPartResponse> UploadPartAsync(
            string bucketName,
            string key,
            string uploadId,
            int partNumber,
            int partSize,
            Stream inputStream,
            Action<object, StreamTransferProgressArgs> progress = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            if (partSize > DefaultPartSize)
                throw new ArgumentException($"Part size in multipart upload can't exceed {DefaultPartSize} B, but was {partSize} B, bucket: {bucketName}, key: {key}, part: {partNumber}");

            var request = new UploadPartRequest()
            {
                BucketName = bucketName,
                Key = key,
                UploadId = uploadId,
                PartNumber = partNumber,
                PartSize = partSize,
                InputStream = inputStream,
            };

            if (progress != null)
                request.StreamTransferProgress += new EventHandler<StreamTransferProgressArgs>(progress);

            return _S3Client.UploadPartAsync(request, cancellationToken).EnsureSuccessAsync();
        }

        public Task<CompleteMultipartUploadResponse> CompleteMultipartUploadAsync(
            string bucketName,
            string key,
            string uploadId,
            IEnumerable<PartETag> partETags,
            CancellationToken cancellationToken = default(CancellationToken))
                => _S3Client.CompleteMultipartUploadAsync(
                new CompleteMultipartUploadRequest()
                {
                    BucketName = bucketName,
                    Key = key,
                    UploadId = uploadId,
                    PartETags = partETags.ToList(),
                }, cancellationToken).EnsureSuccessAsync();


        public async Task<S3Object[]> ListObjectsAsync(string bucketName, string prefix, CancellationToken cancellationToken = default(CancellationToken))
        {
            ListObjectsResponse response = null;
            var results = new List<S3Object>();
            while ((response = await _S3Client.ListObjectsAsync(new ListObjectsRequest()
            {
                Marker = response?.NextMarker,
                BucketName = bucketName,
                Prefix = prefix,
                MaxKeys = 100000,
            }, cancellationToken).EnsureSuccessAsync()) != null)
            {
                if (!response.S3Objects.IsNullOrEmpty())
                    results.AddRange(response.S3Objects);

                if (!response.IsTruncated)
                    break;

                await Task.Delay(100);
            }

            return results.ToArray();
        }

        public async Task<S3ObjectVersion[]> ListVersionsAsync(string bucketName, string prefix, CancellationToken cancellationToken = default(CancellationToken))
        {
            ListVersionsResponse response = null;
            var results = new List<S3ObjectVersion>();
            while ((response = await _S3Client.ListVersionsAsync(new ListVersionsRequest()
            {
                VersionIdMarker = response?.NextVersionIdMarker,
                KeyMarker = response?.NextKeyMarker,
                BucketName = bucketName,
                Prefix = prefix,
                MaxKeys = 100000,
            }, cancellationToken).EnsureSuccessAsync()) != null)
            {
                if (!response.Versions.IsNullOrEmpty())
                    results.AddRange(response.Versions);

                if (!response.IsTruncated)
                    break;

                await Task.Delay(100);
            }

            return results.ToArray();
        }
        
        public async Task<S3Bucket[]> ListBucketsAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            var response = await _S3Client.ListBucketsAsync(new ListBucketsRequest(), cancellationToken).EnsureSuccessAsync();
            return response.Buckets.ToArray();
        }
    }
}
