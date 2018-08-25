using System.Threading.Tasks;
using System.Threading;
using Amazon.EC2.Model;
using AsmodatStandard.Extensions.Collections;
using System.Linq;
using System.Collections.Generic;
using System.Diagnostics;
using System;
using static AWSWrapper.EC2.EC2Helper;
using AsmodatStandard.Extensions;
using Amazon.EC2;

namespace AWSWrapper.EC2
{
    public static class EC2HelperEx
    {
        public static InstanceType ToInstanceType(this InstanceModel model)
        {
            switch(model)
            {
                case InstanceModel.T2Nano: return InstanceType.T2Nano;
                case InstanceModel.T2Micro: return InstanceType.T2Micro;
                case InstanceModel.T2Small: return InstanceType.T2Small;
                case InstanceModel.T2Medium: return InstanceType.T2Medium;
                case InstanceModel.T2Large: return InstanceType.T2Large;
                case InstanceModel.T2XLarge: return InstanceType.T2Xlarge;
                case InstanceModel.T22XLarge: return InstanceType.T22xlarge;
                default: throw new Exception($"Unrecognized instance model: {model.ToString()}");
            }
        }

        public static async Task AwaitInstanceStateCode(this EC2Helper ec2, string instanceId, InstanceStateCode instanceStateCode, int timeout_ms, int intensity = 1500, CancellationToken cancellationToken = default(CancellationToken))
        {
            var sw = Stopwatch.StartNew();
            InstanceStatus status = null;
            do
            {
                if (status != null)
                    await Task.Delay(intensity);

                status = await ec2.DescribeInstanceStatusAsync(instanceId, cancellationToken);
                if (status.InstanceState.Code == (int)instanceStateCode)
                    return;
            }
            while (sw.ElapsedMilliseconds < timeout_ms);

            throw new TimeoutException($"Instance {instanceId} could not reach state code {instanceStateCode.ToString()}, last state: {status?.InstanceState?.Code.ToEnumStringOrDefault<InstanceStateCode>($"<convertion failure of value {status?.InstanceState?.Code}>")}");
        }

        public static string GetTagValueOrDefault(this Instance instance, string key)
            => instance.Tags?.FirstOrDefault(x => x.Key == key)?.Value;

        public static async Task<Instance[]> ListInstances(this EC2Helper ec2, CancellationToken cancellationToken = default(CancellationToken))
        {
            var batch = await ec2.DescribeInstancesAsync(instanceIds: null, filters: null, cancellationToken: cancellationToken);
            return batch.SelectMany(x => x.Instances).ToArray();
        }

        public static async Task<Instance[]> ListInstancesByTagKey(this EC2Helper ec2, string tagKey, CancellationToken cancellationToken = default(CancellationToken))
        {
            var batch = await ec2.DescribeInstancesAsync(instanceIds: null, filters: null, cancellationToken: cancellationToken);
            return batch.SelectMany(x => x.Instances).Where(x => (x?.Tags?.Any(t => t?.Key == tagKey) ?? false) == true).ToArray();
        }

        public static async Task<Instance[]> ListInstancesByName(this EC2Helper ec2, string name, CancellationToken cancellationToken = default(CancellationToken))
        {
            var batch = await ec2.DescribeInstancesAsync(instanceIds: null, filters: null, cancellationToken: cancellationToken);
            return batch.SelectMany(x => x.Instances).Where(x => (x?.Tags?.Any(t => t?.Key?.ToLower() == "name" && t.Value == name) ?? false) == true).ToArray();
        }

        public static async Task<InstanceStateChange> StopInstance(this EC2Helper ec2,string instanceId, bool force = false, CancellationToken cancellationToken = default(CancellationToken))
        {
            var resp = await ec2.StopInstancesAsync(new List<string>() { instanceId }, force: force, cancellationToken: cancellationToken);
            return resp.StoppingInstances.FirstOrDefault(x => x.InstanceId == instanceId);
        }

        public static async Task<InstanceStateChange> StartInstance(this EC2Helper ec2, string instanceId, string additionalInfo = null, CancellationToken cancellationToken = default(CancellationToken))
        {
            var resp = await ec2.StartInstancesAsync(new List<string>() { instanceId }, additionalInfo: additionalInfo, cancellationToken: cancellationToken);
            return resp.StartingInstances.FirstOrDefault(x => x.InstanceId == instanceId);
        }

        public static async Task<InstanceStateChange> TerminateInstance(this EC2Helper ec2, string instanceId, CancellationToken cancellationToken = default(CancellationToken))
        {
            var resp = await ec2.TerminateInstancesAsync(new List<string>() { instanceId }, cancellationToken: cancellationToken);
            return resp.TerminatingInstances.FirstOrDefault(x => x.InstanceId == instanceId);
        }
    }
}
