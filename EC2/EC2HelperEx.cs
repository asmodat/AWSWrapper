using System.Threading.Tasks;
using System.Threading;
using Amazon.EC2.Model;
using AsmodatStandard.Extensions.Collections;
using System.Linq;
using System.Collections.Generic;

namespace AWSWrapper.EC2
{
    public static class EC2HelperEx
    {
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
