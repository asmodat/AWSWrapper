using System;
using AsmodatStandard.Extensions;
using AsmodatStandard.Extensions.Collections;
using System.Linq;
using System.Threading.Tasks;
using AsmodatStandard.Extensions.IO;
using AsmodatStandard.Extensions.Threading;
using AsmodatStandard.Cryptography;
using System.Collections.Concurrent;
using AsmodatStandard.Types;
using System.Collections.Generic;
using System.Threading;
using Amazon.S3.Model;
using AWSWrapper.S3.Models;

namespace AWSWrapper.S3
{
    public static class S3HashStoreStatus
    {
        public static readonly string UploadStatusFilePrefix = "sync-file-upload-";

        /// <summary>
        /// Returns list of s3 status files in ascending order (from oldest to latest)
        /// </summary>
        /// <param name="st"></param>
        /// <returns></returns>
        public static async Task<List<S3Object>> GetStatusList(S3Helper s3h, SyncTarget st, string statusPrefix)
        {
            var cts = new CancellationTokenSource();

            var bkp = st.status.ToBucketKeyPair();
            var prefix = $"{bkp.key}/{statusPrefix}";
            var list = (await s3h.ListObjectsAsync(bkp.bucket, prefix, msTimeout: st.timeout, cancellationToken: cts.Token)
                .TryCatchRetryAsync(maxRepeats: st.retry))
                .SortAscending(x => x.Key.TrimStart(prefix).TrimEnd(".json").ToLongOrDefault(0)).ToList();

            if (cts.IsCancellationRequested)
                return null;

            return list;
        }

        public static async Task<StatusFile> GetStatusFile(S3Helper s3h, SyncTarget st, long minTimestamp, long maxTimestamp)
        {
            var bkp = st.status.ToBucketKeyPair();
            var prefix = $"{bkp.key}/{UploadStatusFilePrefix}";
            var list = await GetStatusList(s3h, st, UploadStatusFilePrefix);

            if (list.IsNullOrEmpty())
                return null;

            list.Reverse();
            foreach (var f in list) //find non obsolete files
            {
                var timestamp = f.Key.TrimStart(prefix).TrimEnd(".json").ToLongOrDefault(0);

                if (timestamp < minTimestamp || timestamp > maxTimestamp)
                    continue;

                var s = await s3h.DownloadJsonAsync<StatusFile>(bkp.bucket, f.Key, throwIfNotFound: false)
                    .Timeout(msTimeout: st.timeout)
                    .TryCatchRetryAsync(maxRepeats: st.retry);

                if (s?.finalized == true)
                    return s;
            }

            return null;
        }

        public static async Task<StatusFile> GetStatusFile(S3Helper s3h, SyncTarget st, string statusPrefix)
        {
            var bkp = st.status.ToBucketKeyPair();
            var prefix = $"{bkp.key}/{statusPrefix}";
            var list = await GetStatusList(s3h, st, statusPrefix);

            var id = list.IsNullOrEmpty() ? 0 : list.Last().Key.TrimStart(prefix).TrimEnd(".json").ToLongOrDefault(0); //latest staus id
            id = id <= 0 ? DateTimeEx.TimestampNow() : id;
            var key = $"{prefix}{id}.json";

            if (list.IsNullOrEmpty() || id <= 0)
            {
                return new StatusFile()
                {
                    id = id.ToString(),
                    timestamp = DateTimeEx.UnixTimestampNow(),
                    bucket = bkp.bucket,
                    key = key,
                    location = $"{bkp.bucket}/{key}",
                    finalized = false,
                    version = 0
                };
            }

            var status = await s3h.DownloadJsonAsync<StatusFile>(bkp.bucket, key, throwIfNotFound: true)
                .Timeout(msTimeout: st.timeout)
                .TryCatchRetryAsync(maxRepeats: st.retry);

            var elapsed = (DateTime.UtcNow - long.Parse(status?.id ?? "0").ToDateTimeFromTimestamp()).TotalSeconds;
            if(status == null || (status.finalized == true && st.retention > 0 && elapsed > st.retention))
            {
                id = DateTimeEx.TimestampNow();
                key = $"{prefix}{id}.json";
                status = new Models.StatusFile()
                {
                    id = id.ToString(),
                    timestamp = DateTimeEx.UnixTimestampNow(),
                    bucket = bkp.bucket,
                    key = key,
                    location = $"{bkp.bucket}/{key}",
                    finalized = false,
                    version = 0
                };
            }

            if (st.cleanup && st.rotation > 0 && list.Count > st.rotation)
            {
                var validStatus = new List<StatusFile>();
                list.Reverse();
                foreach (var f in list) //find non obsolete files
                {
                    var s = await s3h.DownloadJsonAsync<StatusFile>(bkp.bucket, f.Key, throwIfNotFound: false)
                        .Timeout(msTimeout: st.timeout)
                        .TryCatchRetryAsync(maxRepeats: st.retry);

                    if (s == null)
                        continue;

                    if (s.finalized && s.id.ToLongOrDefault(0) > 0)
                        validStatus.Add(s);
                    else if(!s.finalized || status.id == id.ToString())
                        validStatus.Add(s);

                    if (validStatus.Count > st.rotation)
                        break;
                }

                status.obsoletes = list.Where(x => !validStatus.Any(v => v.key.ToLower().Trim() == x.Key.ToLower().Trim()))
                    .Select(x => x.Key).ToArray(); //status files that are obsolete
            }

            return status;
        }

        public static StatusFile ReadSyncFile(this SyncTarget st)
        {
            var ds = PathEx.RuntimeCombine(st.sync, st.id, "sync.json").ToFileInfo();

            if (!ds.Exists)
                return new StatusFile()
                {
                    id = st.id.ToString(),
                    timestamp = DateTimeEx.UnixTimestampNow(),
                    finalized = false,
                    version = 0
                };

            return ds.DeserialiseJson<StatusFile>();
        }

        public static void WriteSyncFile(this SyncTarget st, StatusFile sf)
        {
            var ds = PathEx.RuntimeCombine(st.sync, st.id, "sync.json").ToFileInfo();

            ds.Directory.TryCreate();

            if (ds.TryCreate())
            {
                ds.WriteAllText(sf.JsonSerialize(Newtonsoft.Json.Formatting.Indented));
            }
            else
                throw new Exception($"Failed to create status '{ds?.Directory?.FullName ?? "undefined"}' for the Sync Target '{st?.id ?? "undefined"}'.");
        }

        public static void WriteInfoFile(this SyncTarget st, SyncInfo si)
        {
            var ds = PathEx.RuntimeCombine(st.sync, st.id, "info.json").ToFileInfo();

            ds.Directory.TryCreate();

            if (ds.TryCreate())
            {
                ds.WriteAllText(si.JsonSerialize(Newtonsoft.Json.Formatting.Indented));
            }
            else
                throw new Exception($"Failed to create status '{ds?.Directory?.FullName ?? "undefined"}' for the Sync Target '{st?.id ?? "undefined"}'.");
        }
    }
}
