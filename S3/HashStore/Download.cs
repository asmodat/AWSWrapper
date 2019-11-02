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
        public async Task<SyncInfo> Download()
        {
            var bkp = st.source.ToBucketKeyPair();
            var bucket = bkp.bucket;
            si = new SyncInfo(st);
            si.start = DateTimeEx.UnixTimestampNow();

            if (bucket.IsNullOrEmpty())
                throw new Exception($"Source '{st.source ?? "undefined"}' does not contain bucket name.");

            var destination = st.destination?.ToDirectoryInfo();

            if (destination?.TryCreate() != true)
                throw new Exception($"Destination '{st.destination ?? "undefined"}' does not exist and coudn't be created.");

            if (st.verbose)
                Console.WriteLine($"Processing Download Target: '{st?.id ?? "undefined"}'");

            var status = await S3HashStoreStatus.GetStatusFile(s3h, st, st.minTimestamp, st.maxTimestamp);
            var downloadStatus = st.ReadSyncFile();

            if (status == null)
                throw new Exception($"Could not download latest data from the source '{st.source}', status file was not found in '{st?.status ?? "undefined"}' within time range of <{st.minTimestamp.ToDateTimeFromTimestamp().ToLongDateTimeString()},{st.maxTimestamp.ToDateTimeFromTimestamp().ToLongDateTimeString()}>");

            status.files = status?.files?.Where(x => x != null)?.ToArray() ?? new SilyFileInfo[0];
            si.total = status.files.Sum(x => x?.Length ?? 0);

            if (downloadStatus.finalized)
            {
                var elspased = DateTimeEx.UnixTimestampNow() - si.start;
                if (st.verbose)
                    Console.WriteLine($"Download sync file '{st.status}' was already finalized {elspased}s ago.");
                await Task.Delay(millisecondsDelay: 1000);
                si.success = true;
                return si;
            }

            if (st.verbose)
                Console.WriteLine($"Download Target: '{st?.id ?? "undefined"}' status indicates that targt is not finalized");

            int counter = 0;
            var directories = new List<DirectoryInfo>();
            directories.Add(st.destination.ToDirectoryInfo());
            foreach (var dir in status.directories)
            {
                if (dir == null)
                    continue;

                var relativeDir = dir.FullName.ToRuntimePath().TrimStart(status.source.ToRuntimePath());
                var downloadDir = PathEx.RuntimeCombine(st.destination, relativeDir).ToDirectoryInfo();

                if (!downloadDir.Exists && st.verbose)
                    Console.WriteLine($"Creating Directory [{++counter}/{status.directories.Length}] '{downloadDir.FullName}' ...");

                if (downloadDir?.TryCreate() != true)
                    throw new Exception($"Could not find or create directory '{downloadDir?.FullName ?? "undefined"}'.");

                directories.Add(downloadDir);
            }

            if (st.wipe)
            {
                counter = 0;
                var currentDirectories = st.destination.ToDirectoryInfo().GetDirectories(recursive: st.recursive);
                foreach (var dir in currentDirectories)
                    if (!directories.Any(x => x.FullName == dir.FullName))
                    {
                        Console.WriteLine($"Removing Directory [{++counter}/{currentDirectories.Length - directories.Count}] '{dir.FullName}' ...");
                        dir.Delete(recursive: st.recursive);
                    }
            }

            if (st.verbose)
                Console.WriteLine($"Found {status.files} files and {status.directories} directories for target '{st?.id ?? "undefined"}'.");

            counter = 1;
            var files = new List<FileInfo>();
            await ParallelEx.ForEachAsync(status.files, async file =>
            {
                try
                {
                    var relativePath = file.FullName.ToRuntimePath().TrimStart(status.source.ToRuntimePath());
                    var downloadPath = PathEx.RuntimeCombine(st.destination, relativePath).ToFileInfo();
                    files.Add(downloadPath);

                    if (downloadPath.Exists && downloadPath.MD5().ToHexString() == file.MD5)
                    {
                        if (st.verbose)
                            Console.WriteLine($"Found [{counter}/{status.files.Length}][{file.Length.ToPrettyBytes()}], file '{downloadPath.FullName}' ({file.MD5}) already exists.");
                        return; //file already exists
                    }

                    var key = $"{st.source.TrimEnd('/')}/{file.MD5}".ToBucketKeyPair().key;

                    if (st.verbose) Console.WriteLine($"Downloading [{counter}/{status.files.Length}][{file.Length.ToPrettyBytes()}] '{bucket}/{key}' => '{downloadPath.FullName}' ...");

                    var sw = Stopwatch.StartNew();

                    async Task DownloadFile()
                    {
                        downloadPath.Refresh();
                        if (downloadPath.Exists && downloadPath.TryDelete() != true)
                            throw new Exception($"Obsolete file was found in '{downloadPath?.FullName ?? "undefined"}' but couldn't be deleted.");

                        using (var stream = await s3h.DownloadObjectAsync(bucketName: bucket, key: key, throwIfNotFound: true))
                        {
                            var compressed = file.TryGetProperty("compress") == "zip";

                            if (!downloadPath.Directory.TryCreate())
                                throw new Exception($"Failed to create directory '{downloadPath?.Directory.FullName ?? "undefined"}'.");

                            if (compressed)
                            {
                                if (st.verbose)
                                    Console.WriteLine($"UnZipping '{downloadPath.FullName}' ...");
                                downloadPath.UnZipStream(stream);
                            }
                            else
                            {
                                using (var fs = File.Create(downloadPath.FullName))
                                    stream.CopyTo(fs);
                            }
                        }

                        downloadPath.Refresh();
                        if (!downloadPath.Exists)
                            throw new Exception($"Failed download '{bucket}/{key}'-/-> '{downloadPath.FullName}'.");

                        if (st.verify)
                        {
                            var md5 = downloadPath.MD5().ToHexString();
                            if (md5 != file.MD5)
                                throw new Exception($"Failed download '{bucket}/{key}'-/-> '{downloadPath.FullName}', expected MD5 to be '{md5 ?? "undefined"}' but was '{file.MD5 ?? "undefined"}'.");
                        }

                        await ss.LockAsync(() =>
                        {
                            si.transferred += file.Length;
                        });
                    }

                    await DownloadFile().TryCatchRetryAsync(maxRepeats: st.retry).Timeout(msTimeout: st.timeout);
                }
                finally
                {
                    await ss.LockAsync(() =>
                    {
                        ++counter;
                        si.processed += file.Length;
                        si.progress = ((double)si.processed / si.total) * 100;
                        st.WriteInfoFile(si);
                    });
                }
            }, maxDegreeOfParallelism: st.parallelism);

            if (st.wipe)
            {
                counter = 0;
                var currentFiles = st.destination.ToDirectoryInfo().GetFiles("*", recursive: st.recursive);
                foreach (var file in currentFiles)
                    if (!files.Any(x => x.FullName == file.FullName))
                    {
                        if (st.verbose)
                            Console.WriteLine($"Removing File [{++counter}/{currentFiles.Length - files.Count}] '{file.FullName}' ...");
                        file.Delete();
                    }
            }

            downloadStatus.finalized = true;
            si.stop = DateTimeEx.UnixTimestampNow();
            si.speed = (double)si.transferred / Math.Max(si.stop - si.start, 1);
            si.success = true;

            st.WriteSyncFile(downloadStatus);

            if (st.verbose)
            {
                Console.WriteLine($"SUCCESS, processed '{st.status}', all {status.files.Length} files and {status.directories.Length} directories were updated.");
                Console.WriteLine($"Average Download Speed: {si.speed.ToPrettyBytes()}/s");
            }

            return si;
        }
    }
}
