using System.Threading.Tasks;
using System.Threading;
using Amazon.EC2.Model;
using AsmodatStandard.Extensions.Collections;
using System.Linq;

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
    }
}
