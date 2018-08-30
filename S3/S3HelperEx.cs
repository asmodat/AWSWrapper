using System.Threading.Tasks;
using System.Threading;
using System.Collections.Generic;
using System.IO;
using AsmodatStandard.Extensions.IO;
using Amazon.S3.Model;
using System.Text;
using AsmodatStandard.Extensions;
using AWSWrapper.KMS;
using AsmodatStandard.Extensions.Collections;
using AsmodatStandard.Threading;
using System.Linq;
using System;

namespace AWSWrapper.S3
{
    public static class S3HelperEx
    {
        public static async Task CreateDirectory(this S3Helper s3,
            string bucketName,
            string path,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var key = $"{path.Trim('/')}/";

            if (await s3.ObjectExistsAsync(bucketName: bucketName, key: key))
                return; //already exists

            var stream = new MemoryStream(new byte[0]);

            var obj = await s3.UploadStreamAsync(bucketName: bucketName,
                key: key,
                inputStream: stream,
                cancellationToken: cancellationToken);
        }

            public static async Task<bool> ObjectExistsAsync(this S3Helper s3,
            string bucketName,
            string key,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            try
            {
                var metadata = await s3.GetObjectMetadata(bucketName: bucketName, key: key, cancellationToken: cancellationToken);
                return true;
            }
            catch (Exception ex)
            {
                if (ex is Amazon.S3.AmazonS3Exception &&
                   (ex as Amazon.S3.AmazonS3Exception).StatusCode == System.Net.HttpStatusCode.NotFound)
                    return false;

                throw;
            }
        }

        public static async Task<bool> DeleteVersionedObjectAsync(this S3Helper s3,
            string bucketName,
            string key,
            bool throwOnFailure = true,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var versions = await s3.ListVersionsAsync(
                bucketName: bucketName,
                prefix: key,
                cancellationToken: cancellationToken);

            var keyVersions = versions
                .Where(v => !v.IsLatest)
                .Select(ver => new KeyVersion() { Key = key, VersionId = ver.VersionId })
                .ToArray();

            if (keyVersions.Length > 0)
            {
                var response = await s3.DeleteObjectsAsync(
                            bucketName: bucketName,
                            objects: keyVersions,
                            cancellationToken: cancellationToken);

                if (response.DeleteErrors.Count > 0)
                    if (throwOnFailure)
                        throw new Exception($"Failed to delete all object versions of key '{key}' in bucket '{bucketName}', {response.DeletedObjects.Count} Deleted {response.DeleteErrors.Count} Errors, Delete Errors: {response.DeleteErrors.JsonSerialize(Newtonsoft.Json.Formatting.Indented)}");
                    else
                        return false;

                return await s3.DeleteVersionedObjectAsync(bucketName, key, throwOnFailure, cancellationToken);
            }

            try
            {
                var latest = versions.Single(x => x.IsLatest);
                await s3.DeleteObjectAsync(bucketName: bucketName, key: key, versionId: latest.VersionId, cancellationToken: cancellationToken);

                return true;
            }
            catch
            {
                if (!await s3.ObjectExistsAsync(bucketName, key, cancellationToken))
                    return true;

                if (throwOnFailure)
                    throw;
                else
                    return false;
            }
        }

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

        public static async Task<FileInfo> DownloadObjectAsync(this S3Helper s3,
            string bucketName,
            string key,
            string outputFile,
            string version,
            string eTag,
            bool @override,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var fi = new FileInfo(outputFile);
            if (fi.Exists)
            {
                if (@override)
                    fi.Delete();
                else
                    throw new Exception($"Can't download '{bucketName}/{key}', becuause output file '{fi.FullName}' already exists.");
            }

            var obj = await s3.GetObjectAsync(
                bucketName: bucketName,
                key: key,
                versionId: version,
                eTag: eTag,
                cancellationToken: cancellationToken);

            var buffSize = 1024 * 1024;
            using (var fw = File.Create(outputFile, buffSize))
               await obj.ResponseStream.CopyToAsync(fw, buffSize);

            fi.Refresh();

            return fi;
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
            var blob = inputStream.ToMemoryBlob(maxLength: (s3.MaxSinglePartSize + 1), bufferSize: bufferSize);

            if (blob.Length <= s3.MaxSinglePartSize)
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
                    inputStream: blob.CopyToMemoryStream(bufferSize: (int)blob.Length), //copy so new part can be read at the same time
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
