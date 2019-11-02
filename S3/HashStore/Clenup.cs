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
        public async Task<bool> Cleanup(StatusFile sf)
        {
            if (sf.obsoletes.IsNullOrEmpty())
                return true;

            var bkp = st.status.ToBucketKeyPair();
            var prefix = $"{bkp.key}/{S3HashStoreStatus.UploadStatusFilePrefix}";
            var success = true;

            await ParallelEx.ForEachAsync(sf.obsoletes, async file =>
            {
                var cts = new CancellationTokenSource();

                var id = file.TrimStart(prefix).TrimEnd(".json").ToLongOrDefault(0);
                var folderBKP = st.destination.ToBucketKeyPair();
                var result = await s3h.DeleteObjectAsync(
                    bucketName: folderBKP.bucket,
                    key: file,
                    throwOnFailure: false,
                    cancellationToken: cts.Token).TryCancelAfter(cts.Token, msTimeout: st.timeout);

                if (success)
                    Console.WriteLine($"Status file: '{folderBKP.bucket}/{file}' was removed.");
                else
                    Console.WriteLine($"Failed to remove status file: '{folderBKP.bucket}/{file}'.");

            }, maxDegreeOfParallelism: parallelism);

            return success;
        }
    }
}
