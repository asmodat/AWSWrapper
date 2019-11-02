using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.EC2;
using Amazon.EC2.Model;
using AWSWrapper.Extensions;
using AsmodatStandard.Extensions;
using AsmodatStandard.Extensions.Collections;


namespace AWSWrapper.Extensions
{
    public static class InstanceEx
    {
        public static bool HasTags(this Instance instance)
            => (instance?.Tags).IsNullOrEmpty() == false;

        public static bool IsTerminating(this Instance instance)
            => (instance?.State?.Name == InstanceStateName.ShuttingDown ||
            instance?.State?.Name == InstanceStateName.Terminated);
    }
}
