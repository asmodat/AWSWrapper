using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon;
using Amazon.ECS;
using AsmodatStandard.Extensions;
using AsmodatStandard.Threading;
using AsmodatStandard.Extensions.Collections;
using System.Threading;

namespace AWSWrapper.CloudWatch
{
    public static class CloudWatchHelperEx
    {
        public static Task DeleteLogGroupAsync(this CloudWatchHelper cwh, 
            string name, 
            bool throwIfNotFound = true, 
            CancellationToken cancellationToken = default(CancellationToken))
                => cwh.DeleteLogGroupsAsync(new string[] { name }, throwIfNotFound, cancellationToken);
    }
}
