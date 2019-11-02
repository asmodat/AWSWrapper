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
using AsmodatStandard.Extensions.Threading;
using System.Security.Cryptography;
using AWSWrapper.S3.Models;
using System.Diagnostics;
using AsmodatStandard.Cryptography;
using AsmodatStandard.Types;

namespace AWSWrapper.S3
{
    public partial class S3HashStore
    {
        public async Task<SyncInfo> Upload()
        {
            var si = new SyncInfo(st);
            var bkp = st.destination.ToBucketKeyPair();
            var bucket = bkp.bucket;
            var key = bkp.key;
            si.start = DateTimeEx.UnixTimestampNow();

            if (bucket.IsNullOrEmpty())
                throw new Exception($"Destination '{st.destination ?? "undefined"}' does not contain bucket name.");

            var path = st.destination;
            var sourceInfo = st.GetSourceInfo();

            if (sourceInfo.rootDirectory == null)
                return si; 

            var directory = st.source.ToDirectoryInfo();
            var prefix = directory.FullName;
            var counter = 0;

            var status = await S3HashStoreStatus.GetStatusFile(s3h, st, S3HashStoreStatus.UploadStatusFilePrefix);
            var elspased = DateTimeEx.UnixTimestampNow() - status.timestamp;

            if (status.finalized)
            {
                var remaining = st.retention - elspased;
                Console.WriteLine($"Upload sync file '{st.status}' was already finalized {elspased}s ago. Next sync in {st.retention - elspased}s.");
                await Task.Delay(millisecondsDelay: 1000);
                si.success = true;
                return si;
            }

            si.total = sourceInfo.files.Sum(x => x?.Length ?? 0);
            var cleanup = st.cleanup ? Cleanup(status) : null;
            var isStatusFileUpdated = false;
            var files = new List<SilyFileInfo>();
            var uploadedBytes = new List<long>();
            double compressionSum = 0;

            await ParallelEx.ForEachAsync(sourceInfo.files, async file =>
            {
                double compression = 1;

                try
                {
                    var sw = Stopwatch.StartNew();
                    var uploadedFile = status.files?.FirstOrDefault(x => x.FullNameEqual(file));

                    string localMD5;
                    string destination;
                    if (uploadedFile != null) //file was already uploaded to AWS
                    {
                        if (uploadedFile.LastWriteTime == file.LastWriteTime.ToUnixTimestamp())
                        {
                            if (st.verbose)
                                Console.WriteLine($"Skipping upload of '{file.FullName}', file did not changed since last upload.");

                            await ss.LockAsync(() =>
                                {
                                    files.Add(uploadedFile);
                                    ++counter;
                                });
                            return; //do not uplad, file did not changed
                        }

                        localMD5 = file.MD5().ToHexString();
                        destination = $"{key}/{localMD5}";
                        if (localMD5 == uploadedFile.MD5)
                        {
                            if (st.verbose)
                                Console.WriteLine($"Skipping upload of '{file.FullName}', file alredy exists in the '{bucket}/{destination}'.");

                            await ss.LockAsync(() =>
                            {
                                ++counter;
                                files.Add(uploadedFile);
                            });
                            return;
                        }
                    }
                    else //file was not uploaded to AWS yet
                    {
                        localMD5 = file.MD5().ToHexString();
                        destination = $"{key}/{localMD5}";
                        var metadata = await s3h.ObjectMetadataAsync(
                            bucketName: bucket,
                            key: $"{key}/{localMD5}",
                            throwIfNotFound: false)
                            .Timeout(msTimeout: st.timeout)
                            .TryCatchRetryAsync(maxRepeats: st.retry);

                        if (metadata != null) //file exists
                        {
                            await ss.LockAsync(() =>
                            {
                                ++counter;
                                var sfi = file.ToSilyFileInfo(md5: localMD5);

                                if (sfi.Length >= (metadata.ContentLength + 128))
                                    sfi.TrySetProperty("compress", "zip");

                                files.Add(sfi);
                            });

                            if (st.verbose)
                                Console.WriteLine($"Skipping upload of '{file.FullName}', file was found in the '{bucket}/{destination}'.");
                            return;
                        }
                    }

                    await ss.LockAsync(async () =>
                    {
                        if (!isStatusFileUpdated) //update status file
                        {
                            status.timestamp = si.start;
                            status.version = status.version + 1;
                            status.finalized = false;
                            var statusUploadResult = await s3h.UploadJsonAsync(status.bucket, status.key, status)
                                .Timeout(msTimeout: st.timeout)
                                .TryCatchRetryAsync(maxRepeats: st.retry);

                            isStatusFileUpdated = true;
                        }

                        ++counter;
                    });

                    async Task<string> UploadFile()
                    {
                        file?.Refresh();
                        if (file == null || !file.Exists)
                            return null;

                        var shareMode = EnumEx.ToEnum<FileShare>(st.filesShare);

                        FileInfo compressedFile = null;

                        await ss.LockAsync(() =>
                        {
                            if (st.compress)
                            {
                                compressedFile = PathEx.RuntimeCombine(st.sync, localMD5).ToFileInfo();
                                file.Zip(compressedFile);
                                compressedFile.Refresh();

                                if ((compressedFile.Length + 128) < file.Length)
                                    compression = (double)compressedFile.Length / Math.Max(file.Length, 1);
                                else
                                    compression = 1;
                            }
                        });

                        FileStream fs = null;

                        await ss.LockAsync(() =>
                        {
                            fs = File.Open( //upload new file to AWS
                                compression < 1 ? compressedFile.FullName : file.FullName,
                                FileMode.Open,
                                FileAccess.Read,
                                shareMode);
                        });

                        var hash = await s3h.UploadStreamAsync(bucketName: bucket,
                             key: destination,
                             inputStream: fs,
                             throwIfAlreadyExists: false, msTimeout: st.timeout).TryCatchRetryAsync(maxRepeats: st.retry);

                        fs.Close();

                        if (!compressedFile.TryDelete())
                            throw new Exception($"Failed to remove temporary file '{compressedFile?.FullName ?? "undefined"}' after deletion.");

                        return hash.IsNullOrEmpty() ? null : hash;
                    }

                    if (st.verbose)
                        Console.WriteLine($"Uploading [{counter}/{sourceInfo.files.Length}][{file.Length.ToPrettyBytes()}] '{file.FullName}' => '{bucket}/{destination}' ...");

                    var md5 = await UploadFile().TryCatchRetryAsync(maxRepeats: st.retry).Timeout(msTimeout: st.timeout);

                    if (md5.IsNullOrEmpty())
                        throw new Exception($"FAILED, Upload '{file.FullName}' => '{bucket}/{destination}'");

                    var silyFile = file.ToSilyFileInfo(localMD5);

                    if (compression < 1)
                    {
                        if (st.verbose)
                            Console.WriteLine($"File size reduced by [{compression * 100:0.00} %], file: '{file.FullName}' ({md5})");
                        silyFile.TrySetProperty("compress", "zip");
                        compressionSum += compression;
                    }
                    else
                    {
                        if (md5 != localMD5 && st.verbose)
                            Console.WriteLine($"Warning! file hash changed during upload '{file.FullName}' {localMD5} => {md5}.");

                        compressionSum += 1;
                    }

                    await ss.LockAsync(() =>
                    {
                        files.Add(silyFile);
                        si.transferred += (long)(file.Length * compressionSum);
                    });
                }
                finally
                {
                    await ss.LockAsync(() =>
                    {
                        si.processed += file.Length;
                        si.progress = ((double)si.processed / si.total) * 100;
                        st.WriteInfoFile(si);
                    });
                }
            }, maxDegreeOfParallelism: st.parallelism);

            var directories = sourceInfo.directories.Select(x => x.ToSilyDirectoryInfo()).ToArray();
            si.speed = (double)si.transferred / Math.Max(si.stop - si.start, 1);
            si.success = true;
            si.stop = DateTimeEx.UnixTimestampNow();
            si.compression = (double)si.transferred / si.total;

            if (cleanup != null)
                await cleanup;

            if (isStatusFileUpdated || //if modifications were made to files
                !status.directories.JsonEquals(directories)) // or directories
            {
                status.files = files.ToArray();
                status.finalized = true;
                status.directories = directories;
                status.source = st.source;
                status.destination = st.destination;
                var uploadResult = await s3h.UploadJsonAsync(status.bucket, status.key, status)
                    .Timeout(msTimeout: st.timeout)
                    .TryCatchRetryAsync(maxRepeats: st.retry);

                if (st.verbose)
                {
                    Console.WriteLine($"SUCCESS, processed '{st.status}', all {status.files.Length} files and {status.directories.Length} directories were updated.");
                    Console.WriteLine($"Uploaded {si.transferred.ToPrettyBytes()}, Speed: {si.speed.ToPrettyBytes()}/s, Compressed: {si.compression*100:0.00}%");
                }
            }
            
            return si;
        }
    }
}
