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
using System.Linq;
using System;
using AsmodatStandard.Extensions.Threading;
using System.Security.Cryptography;

namespace AWSWrapper.S3
{
    public static partial class S3HelperEx
    {
        public static (string bucket, string key) ToBucketKeyPair(this string path)
        {
            path = path?.Trim().TrimStartMany("/", "\\");

            if (path.IsNullOrEmpty())
                throw new ArgumentNullException($"Splitting bucket and key failed, path can't be null or empty, but was '{path}'");

            if (!path.Contains('/'))
                return (path, null);
            
            var bucket = path.SplitByFirst('/').FirstOrDefault();
            var key = path.TrimStartSingle(bucket).Trim("/");
            return (bucket, key);
        }

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

        public static async Task<GetObjectMetadataResponse> ObjectMetadataAsync(this S3Helper s3,
            string bucketName,
            string key,
            bool throwIfNotFound = true,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            try
            {
                var metadata = await s3.GetObjectMetadata(bucketName: bucketName, key: key, cancellationToken: cancellationToken);
                return metadata;
            }
            catch (Exception ex)
            {
                if (!throwIfNotFound)
                {
                    if (ex is Amazon.S3.AmazonS3Exception &&
                       (ex as Amazon.S3.AmazonS3Exception).StatusCode == System.Net.HttpStatusCode.NotFound)
                        return null;
                }

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
            bool throwIfNotFound = true,
            Encoding encoding = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var stream = await s3.DownloadObjectAsync(
                bucketName: bucketName,
                key: key,
                version: version,
                eTag: eTag,
                throwIfNotFound: throwIfNotFound,
                cancellationToken: cancellationToken);

            if (!throwIfNotFound && stream == null)
                return null;

            return (encoding ?? Encoding.UTF8).GetString(stream.ToArray(bufferSize: 256 * 1024));
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

            var stream = await DownloadObjectAsync(
                s3: s3,
                bucketName: bucketName,
                key: key,
                version: version,
                eTag: eTag,
                cancellationToken: cancellationToken);

            var buffSize = 1024 * 1024;
            using (var fw = File.Create(outputFile, buffSize))
               await stream.CopyToAsync(fw, buffSize);

            fi.Refresh();

            return fi;
        }

        public static async Task<Stream> DownloadObjectAsync(this S3Helper s3,
            string bucketName,
            string key,
            string version = null,
            string eTag = null,
            bool throwIfNotFound = true,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            if (!throwIfNotFound && !await s3.ObjectExistsAsync(bucketName: bucketName, key: key))
                return null;

            var obj = await s3.GetObjectAsync(
                bucketName: bucketName,
                key: key,
                versionId: version,
                eTag: eTag,
                cancellationToken: cancellationToken);

            return obj.ResponseStream;
        }

        public static async Task<T> DownloadJsonAsync<T>(this S3Helper s3,
            string bucketName,
            string key,
            string version = null,
            string eTag = null,
            bool throwIfNotFound = true,
            Encoding encoding = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var stream = await s3.DownloadObjectAsync(
                bucketName: bucketName,
                key: key,
                version: version,
                eTag: eTag,
                throwIfNotFound: throwIfNotFound,
                cancellationToken: cancellationToken);

            if (!throwIfNotFound && stream == null)
                return default(T);

            return await stream
                .ToStringAsync(encoding ?? Encoding.UTF8)
                .JsonDeserializeAsync<T>();
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
                contentType: "text/plain",
                cancellationToken: cancellationToken);

        

        public static Task<string> UploadJsonAsync<T>(this S3Helper s3,
            string bucketName,
            string key,
            T content,
            Newtonsoft.Json.Formatting formatting = Newtonsoft.Json.Formatting.Indented,
            string keyId = null,
            Encoding encoding = null,
            CancellationToken cancellationToken = default(CancellationToken))
                => s3.UploadStreamAsync(bucketName: bucketName,
                key: key,
                inputStream: content.JsonSerialize(formatting).ToMemoryStream(encoding),
                contentType: "text/plain",
                cancellationToken: cancellationToken);

        public static async Task<string> UploadStreamAsync(this S3Helper s3,
        string bucketName,
        string key,
        Stream inputStream,
        string keyId = null,
        string contentType = "application/octet-stream",
        bool throwIfAlreadyExists = false,
        int msTimeout = int.MaxValue,
        CancellationToken cancellationToken = default(CancellationToken))
        {
            CancellationToken ct;
            void UpdateCancellationToken() {
                if (cancellationToken != null)
                    ct = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken).Token;
                else
                    ct = new CancellationTokenSource().Token;
            }
            UpdateCancellationToken();

            if (throwIfAlreadyExists &&
                await s3.ObjectExistsAsync(bucketName: bucketName, key: key, cancellationToken: ct)
                .TryCancelAfter(ct, msTimeout: msTimeout))
                throw new Exception($"Object {key} in bucket {bucketName} already exists.");

            if (keyId == "")
                keyId = null;
            
            if(keyId != null && !keyId.IsGuid())
            {
                UpdateCancellationToken();
                var alias = await (new KMSHelper(s3._credentials)).GetKeyAliasByNameAsync(name: keyId, cancellationToken: ct)
                    .TryCancelAfter(ct, msTimeout: msTimeout);
                keyId = alias.TargetKeyId;
            }

            var bufferSize = 128 * 1024;
            var blob = inputStream.ToMemoryBlob(maxLength: s3.MaxSinglePartSize, bufferSize: bufferSize);
            var ih = IncrementalHash.CreateHash(HashAlgorithmName.MD5);
            string md5, etag;

            if (blob.Length < s3.MaxSinglePartSize)
            {
                UpdateCancellationToken();
                using (var ms = blob.CopyToMemoryStream(bufferSize: (int)blob.Length))
                {
                    var spResult = s3.PutObjectAsync(bucketName: bucketName, key: key, inputStream: ms, keyId: keyId, cancellationToken: ct, contentType: contentType)
                        .TryCancelAfter(ct, msTimeout: msTimeout);

                    blob.Seek(0, SeekOrigin.Begin);
                    ih.AppendData(blob.ToArray());

                    md5 = ih.GetHashAndReset().ToHexString();
                    etag = (await spResult).ETag.Trim('"');
                    return md5;
                }
            }

            UpdateCancellationToken();
            var init = await s3.InitiateMultipartUploadAsync(bucketName, key, contentType: contentType, keyId: keyId, cancellationToken: ct)
                .TryCancelAfter(ct, msTimeout: msTimeout);
            var partNumber = 0;
            var tags = new List<PartETag>();
            while (blob.Length > 0)
            {
                partNumber = ++partNumber;
                UpdateCancellationToken();

                //copy so new part can be read at the same time
                using (var ms = blob.CopyToMemoryStream(bufferSize: (int)blob.Length))
                {
                    var tUpload = s3.UploadPartAsync(
                        bucketName: bucketName,
                        key: key,
                        uploadId: init.UploadId,
                        partNumber: partNumber,
                        partSize: (int)ms.Length,
                        inputStream: ms,
                        progress: null,
                        cancellationToken: ct).TryCancelAfter(ct, msTimeout: msTimeout);

                    if (ct.IsCancellationRequested)
                        throw new OperationCanceledException("Operation was cancelled or timed out.");

                    if (blob.Length <= s3.DefaultPartSize) //read next part from input before stream gets uploaded
                        blob = inputStream.ToMemoryBlob(maxLength: s3.DefaultPartSize, bufferSize: bufferSize);

                    tags.Add(new PartETag(partNumber, (await tUpload).ETag));

                    ms.Seek(0, SeekOrigin.Begin);
                    ih.AppendData(ms.ToArray());
                }
            }

            UpdateCancellationToken();
            var mpResult = await s3.CompleteMultipartUploadAsync(
                bucketName: bucketName,
                key: key,
                uploadId: init.UploadId,
                partETags: tags,
                cancellationToken: ct).TryCancelAfter(ct, msTimeout: msTimeout);

            md5 = ih.GetHashAndReset().ToHexString();
            etag = mpResult.ETag.Trim('"');
            return md5;
        }

        public static async Task<DeleteObjectsResponse> DeleteObjectsModifiedBeforeDateAsync(this S3Helper s3,
            string bucketName,
            string prefix,
            DateTime dt,
            bool throwIfNotFound,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var list = await s3.ListObjectsAsync(bucketName: bucketName, prefix: prefix, cancellationToken: cancellationToken);
            var toDelete = list.Where(x => x.LastModified.Ticks < dt.Ticks).Select(x => new KeyVersion() {
                Key = x.Key,
            }).ToArray();

            if (toDelete.IsNullOrEmpty())
            {
                if (throwIfNotFound)
                    throw new Exception($"Found {list?.Length ?? 0} objects with prefix {prefix} in bucket {bucketName}, but none were marked for deletion.");
                else
                    return null;
            }

            var deleted = await s3.DeleteObjectsAsync(bucketName: bucketName, objects: toDelete, cancellationToken: cancellationToken);

            if (!deleted.DeleteErrors.IsNullOrEmpty())
                throw new Exception($"Deleted {deleted.DeletedObjects.Count} objects, but failed {deleted.DeleteErrors.Count}, due to following errors: {deleted.DeleteErrors.JsonSerialize()}");

            return deleted;
        }
    }
}
