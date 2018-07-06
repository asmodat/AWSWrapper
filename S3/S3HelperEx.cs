using System.Threading.Tasks;
using System.Threading;
using Amazon.IdentityManagement.Model;
using AsmodatStandard.Extensions.Collections;
using System.Linq;
using System.Collections.Generic;
using System;
using AsmodatStandard.Threading;
using System.IO;
using AsmodatStandard.Extensions.IO;
using Amazon.S3.Model;
using System.Text;
using AsmodatStandard.Extensions;

namespace AWSWrapper.S3
{
    public static class S3HelperEx
    {
        public static Task<CompleteMultipartUploadResponse> UploadTextAsync(this S3Helper s3,
            string bucketName,
            string key,
            string text,
            Encoding encoding = null,
            CancellationToken cancellationToken = default(CancellationToken))
                => s3.UploadStreamAsync(bucketName: bucketName,
                key: key,
                inputStream: text.ToMemoryStream(encoding),
                contentType: "plain/text",
                cancellationToken: cancellationToken);

        public static async Task<CompleteMultipartUploadResponse> UploadStreamAsync(this S3Helper s3,
        string bucketName,
        string key,
        Stream inputStream,
        string contentType = "application/octet-stream",
        CancellationToken cancellationToken = default(CancellationToken))
        {
            var bufferSize = 128 * 1024;
            var tInit = s3.InitiateMultipartUploadAsync(bucketName, key, contentType, cancellationToken);
            var blob = inputStream.ToMemoryBlob(maxLength: s3.DefaultPartSize, bufferSize: bufferSize);
            var uploadId = (await tInit).UploadId;
            var partNumber = 0;
            var tags = new List<PartETag>();
            while (blob.Length > 0)
            {
                partNumber = ++partNumber;

                var tUpload = s3.UploadPartAsync(
                    bucketName: bucketName,
                    key: key,
                    uploadId: uploadId,
                    partNumber: partNumber,
                    partSize: (int)blob.Length,
                    inputStream: blob.CopyToMemoryStream(bufferSize: bufferSize), //copy so new part can be read at the same time
                    progress: null,
                    cancellationToken: cancellationToken);

                if (blob.Length < s3.DefaultPartSize) //read next part from input before stream gets uploaded
                    blob = inputStream.ToMemoryBlob(maxLength: s3.DefaultPartSize, bufferSize: bufferSize);

                tags.Add(new PartETag(partNumber, (await tUpload).ETag));
            }

            var result = await s3.CompleteMultipartUploadAsync(
                bucketName: bucketName,
                key: key,
                uploadId: uploadId,
                partETags: tags,
                cancellationToken: cancellationToken);

            return result;
        }
    }
}
