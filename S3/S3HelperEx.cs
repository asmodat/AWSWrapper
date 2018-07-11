using System.Threading.Tasks;
using System.Threading;
using System.Collections.Generic;
using System.IO;
using AsmodatStandard.Extensions.IO;
using Amazon.S3.Model;
using System.Text;
using AsmodatStandard.Extensions;
using AWSWrapper.KMS;

namespace AWSWrapper.S3
{
    public static class S3HelperEx
    {
        public static async Task<string> DownloadTextAsync(this S3Helper s3,
            string bucketName,
            string key,
            string version = null,
            string eTag = null,
            Encoding encoding = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var obj = await s3.GetObjectAsync(
                bucketName: bucketName,
                key: key,
                versionId: version,
                eTag: eTag,
                cancellationToken: cancellationToken);

            return (encoding ?? Encoding.UTF8).GetString(obj.ResponseStream.ToArray(bufferSize: 256 * 1024));
        }

        public static Task<string> UploadTextAsync(this S3Helper s3,
            string bucketName,
            string key,
            string text,
            string keyId = null,
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
        string keyId = null,
        string contentType = "application/octet-stream",
        CancellationToken cancellationToken = default(CancellationToken))
        {
            if (keyId == "")
                keyId = null;
            
            if(keyId != null && !keyId.IsGuid())
            {
                var alias = await (new KMSHelper(s3._credentials)).GetKeyAliasByNameAsync(name: keyId, cancellationToken: cancellationToken);
                keyId = alias.TargetKeyId;
            }

            var bufferSize = 128 * 1024;
            var blob = inputStream.ToMemoryBlob(maxLength: s3.DefaultPartSize, bufferSize: bufferSize);

            if (blob.Length < s3.MaxSinglePartSize)
            {
                var spResult = await s3.PutObjectAsync(bucketName: bucketName, key: key, inputStream: blob, keyId: keyId, cancellationToken: cancellationToken);
                return spResult.ETag.Trim('"');
            }
            
            var init = await s3.InitiateMultipartUploadAsync(bucketName, key, contentType, keyId: keyId, cancellationToken: cancellationToken);
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
