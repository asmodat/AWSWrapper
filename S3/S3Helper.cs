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

namespace AWSWrapper.S3
{
    public partial class S3Helper
    {
        public readonly int DefaultPartSize = 5*1024*1025;
        internal readonly int _maxDegreeOfParalelism;
        internal readonly AmazonS3Client _S3Client;

        public S3Helper(int maxDegreeOfParalelism = 8)
        {
            _maxDegreeOfParalelism = maxDegreeOfParalelism;
            _S3Client = new AmazonS3Client();
        }

        public Task<DeleteObjectsResponse> DeleteObjectsAsync(
            string bucketName,
            IEnumerable<KeyVersion> objects,
            CancellationToken cancellationToken = default(CancellationToken))
            => _S3Client.DeleteObjectsAsync(
                new DeleteObjectsRequest() { BucketName = bucketName, RequestPayer = RequestPayer.Requester, Quiet = false, Objects = objects.ToList() },
                cancellationToken).EnsureSuccessAsync();

        public Task<InitiateMultipartUploadResponse> InitiateMultipartUploadAsync(
            string bucketName,
            string key,
            string contentType,
            CancellationToken cancellationToken = default(CancellationToken))
            => _S3Client.InitiateMultipartUploadAsync(
                new InitiateMultipartUploadRequest() { BucketName = bucketName, Key = key, ContentType = contentType }, 
                cancellationToken).EnsureSuccessAsync();

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
                InputStream = inputStream
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
        {
            var request = new CompleteMultipartUploadRequest()
            {
                BucketName = bucketName,
                Key = key,
                UploadId = uploadId
            };

            request.AddPartETags(partETags.ToArray());

            return _S3Client.CompleteMultipartUploadAsync(
                request,
                cancellationToken).EnsureSuccessAsync();
        }

        public async Task<S3Object[]> ListObjectsAsync(string bucketName, string prefix, CancellationToken cancellationToken = default(CancellationToken))
        {
            string nextToken = null;
            ListObjectsResponse response;
            var results = new List<S3Object>();
            while ((response = await _S3Client.ListObjectsAsync(new ListObjectsRequest()
            {
                Marker = nextToken,
                BucketName = bucketName,
                Prefix = prefix,
                MaxKeys = 100000,
            }, cancellationToken).EnsureSuccessAsync()) != null)
            {
                if ((response.S3Objects?.Count ?? 0) == 0)
                    break;

                results.AddRange(response.S3Objects);

                if (response.NextMarker.IsNullOrEmpty())
                    break;

                nextToken = response.NextMarker;
            }

            return results.ToArray();
        }

        public async Task<S3Bucket[]> ListBucketsAsync(string bucketName, string prefix, CancellationToken cancellationToken = default(CancellationToken))
        {
            var response = await _S3Client.ListBucketsAsync(new ListBucketsRequest(), cancellationToken).EnsureSuccessAsync();
            return response.Buckets.ToArray();
        }
    }
}
