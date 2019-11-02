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
        //private static readonly object _locker = new object();
        private static SemaphoreSlim ss = new SemaphoreSlim(1, 1); 
        private int parallelism = 1;
        private SyncTarget st;
        private S3Helper s3h;
        private SyncInfo si;

        public S3HashStore(SyncTarget st)
        {
            parallelism = st?.parallelism ?? 1;
            this.st = st;
            this.si = null;

            this.s3h = (st?.profile).IsNullOrEmpty() ?
                new S3Helper() :
                new S3Helper(Extensions.Helper.GetAWSCredentials(st.profile));
        }

        public SyncInfo GetSyncInfo() => si?.DeepCopy();

        public Task<SyncInfo> Process()
        {
            if (st?.verbose == true)
                Console.WriteLine($"Processing sync target {st?.id ?? "undefined"}");

            if (st?.type == SyncTarget.types.upload)
            {
                return this.Upload();
            }
            else if (st?.type == SyncTarget.types.download)
            {
                return this.Download();
            }
            else
                throw new Exception($"Unknown sync info type: '{(si?.type).ToString() ?? "undefined"}'");
        }
    }
}
