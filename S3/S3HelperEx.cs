using System.Threading.Tasks;
using System.Threading;
using System.Collections.Generic;
using System.IO;
using AsmodatStandard.Extensions.IO;
using Amazon.S3.Model;
using System.Text;
using AsmodatStandard.Extensions;

namespace AWSWrapper.S3
{
    public static class S3HelperEx
    {
        public static Task<string> UploadTextAsync(this S3Helper s3,
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

        public static async Task<string> UploadStreamAsync(this S3Helper s3,
        string bucketName,
        string key,
        Stream inputStream,
        string contentType = "application/octet-stream",
        CancellationToken cancellationToken = default(CancellationToken))
        {
            var bufferSize = 128 * 1024;
            var blob = inputStream.ToMemoryBlob(maxLength: s3.DefaultPartSize, bufferSize: bufferSize);

            if (blob.Length < s3.MaxSinglePartSize)
            {
                var spResult = await s3.PutObjectAsync(bucketName: bucketName, key: key, inputStream: blob, cancellationToken: cancellationToken);
                return spResult.ETag.Trim('"');
            }
            
            var init = await s3.InitiateMultipartUploadAsync(bucketName, key, contentType, cancellationToken);
            var partNumber = 0;
            var tags = new List<PartETag>();
            while (blob.Length > 0)
            {
                partNumber = ++partNumber;

                var tUpload = s3.UploadPartAsync(
                    bucketName: bucketName,
                    key: key,
                    uploadId: init.UploadId,
                    partNumber: partNumber,
                    partSize: (int)blob.Length,
                    inputStream: blob.CopyToMemoryStream(bufferSize: bufferSize), //copy so new part can be read at the same time
                    progress: null,
                    cancellationToken: cancellationToken);

                if (blob.Length < s3.DefaultPartSize) //read next part from input before stream gets uploaded
                    blob = inputStream.ToMemoryBlob(maxLength: s3.DefaultPartSize, bufferSize: bufferSize);

                tags.Add(new PartETag(partNumber, (await tUpload).ETag));
            }

            var mpResult = await s3.CompleteMultipartUploadAsync(
                bucketName: bucketName,
                key: key,
                uploadId: init.UploadId,
                partETags: tags,
                cancellationToken: cancellationToken);

            return mpResult.ETag.Trim('"');
        }
    }
}
