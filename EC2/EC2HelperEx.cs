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
using AsmodatStandard.Extensions.Net;
using AsmodatStandard.Extensions.Threading;

namespace AWSWrapper.EC2
{
    public static class EC2HelperEx
    {
        public static Task<string> GetEnvironmentRegion(int timeoutSeconds = 5)
        {
            var url = "http://169.254.169.254/latest/meta-data/placement/availability-zone";
            return HttpHelper.GET(url, timeoutSeconds: timeoutSeconds);
        }

        public static Task<string> GetEnvironmentInstanceId(int timeoutSeconds = 5)
        {
            var url = "http://169.254.169.254/latest/meta-data/instance-id";
            return HttpHelper.GET(url, timeoutSeconds: timeoutSeconds);
        }

        public static async Task<Dictionary<string, string>> GetEnvironmentTags(
            string instanceId = null,
            bool throwIfNotFound = true,
            int timeoutSeconds = 10)
        {
            if(instanceId.IsNullOrEmpty())
                instanceId = await GetEnvironmentInstanceId(timeoutSeconds: (timeoutSeconds/2));

            //Console.WriteLine($"Fetching tag's from instance {instanceId ?? "undefined"} in region {region ?? "undefined"}");
            var ec2 = new EC2Helper();
            var instance = await ec2.GetInstanceById(instanceId: instanceId, throwIfNotFound: throwIfNotFound);
            
            return instance?.Tags?.ToDictionary(x => x.Key, y => y.Value);
        }

        public static async Task<Dictionary<string, string>> TryGetEnvironmentTags(
            string instanceId = null,
            bool throwIfNotFound = true,
            int timeoutSeconds = 10)
        {
            try
            {
                return await GetEnvironmentTags(
                    instanceId: instanceId,
                    throwIfNotFound: throwIfNotFound).Timeout(msTimeout: (timeoutSeconds * 1000));
            }
            catch
            {
                if (throwIfNotFound)
                    throw;

                return null;
            }
        }

        public static InstanceType ToInstanceType(this string model)
        {
            model = model?.ReplaceMany((" ", ""), (".", ""))?.ToLower();
            if(model.IsNullOrEmpty())
                throw new Exception($"Model was not defined.");

            var models = EnumEx.ToStringArray<InstanceModel>();
            foreach(var m in models)
            {
                var tmp = m?.ReplaceMany((" ", ""), (".", ""))?.ToLower();
                if (tmp == model)
                    return ToInstanceType(EnumEx.ToEnum<InstanceModel>(m));
            } 

            throw new Exception($"Model '{model}' was not found, should be one of: {models?.JsonSerialize() ?? "undefined"}.");
        }

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
                case InstanceModel.T3Nano: return InstanceType.T3Nano;
                case InstanceModel.T3Micro: return InstanceType.T3Micro;
                case InstanceModel.T3Small: return InstanceType.T3Small;
                case InstanceModel.T3Medium: return InstanceType.T3Medium;
                case InstanceModel.T3Large: return InstanceType.T3Large;
                case InstanceModel.T3XLarge: return InstanceType.T3Xlarge;
                case InstanceModel.T32XLarge: return InstanceType.T32xlarge;
                case InstanceModel.T3aNano: return InstanceType.T3aNano;
                case InstanceModel.T3aMicro: return InstanceType.T3aMicro;
                case InstanceModel.T3aSmall: return InstanceType.T3aSmall;
                case InstanceModel.T3aMedium: return InstanceType.T3aMedium;
                case InstanceModel.T3aLarge: return InstanceType.T3aLarge;
                case InstanceModel.T3aXLarge: return InstanceType.T3aXlarge;
                case InstanceModel.T3a2XLarge: return InstanceType.T3a2xlarge;
                case InstanceModel.C5Large: return InstanceType.C5Large;
                case InstanceModel.C5XLarge: return InstanceType.C5Xlarge;
                case InstanceModel.C52XLarge: return InstanceType.C52xlarge;
                default: throw new Exception($"Unrecognized instance model: {model.ToString()}");
            }
        }

        public static SummaryStatus ToSummaryStatus(this InstanceSummaryStatus status)
        {
            switch (status)
            {
                case InstanceSummaryStatus.Initializing: return SummaryStatus.Initializing;
                case InstanceSummaryStatus.InsufficientData: return SummaryStatus.InsufficientData;
                case InstanceSummaryStatus.NotApplicable: return SummaryStatus.NotApplicable;
                case InstanceSummaryStatus.Ok: return SummaryStatus.Ok;
                default: throw new Exception($"Unrecognized instance summary status: {status.ToString()}");
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

        public static async Task AwaitInstanceStatus(this EC2Helper ec2, 
            string instanceId, 
            InstanceSummaryStatus summaryStatus, 
            int timeout_ms, int intensity = 1500,
            bool thowOnTermination = true,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var sw = Stopwatch.StartNew();
            InstanceStatus status = null;
            do
            {
                if (status != null)
                    await Task.Delay(intensity);

                status = await ec2.DescribeInstanceStatusAsync(instanceId, cancellationToken);
                if (status.Status.Status == summaryStatus.ToSummaryStatus())
                    return;

                if (thowOnTermination &&
                    (status.InstanceState?.Name == InstanceStateName.Stopping ||
                    status.InstanceState?.Name == InstanceStateName.Terminated))
                    throw new Exception($"Failed Status Await, Instane is terminated or terminating: '{status.InstanceState.Name}'");
            }
            while (sw.ElapsedMilliseconds < timeout_ms);

            throw new TimeoutException($"Instance {instanceId} could not reach status summary '{summaryStatus}', last status summary: '{status.Status.Status}'");
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

        public static async Task<Instance[]> ListInstancesByName(
            this EC2Helper ec2, 
            string name, 
            IEnumerable<InstanceStateName> stateExclude = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var batch = await ec2.DescribeInstancesAsync(instanceIds: null, filters: null, cancellationToken: cancellationToken);
            var instances = batch.SelectMany(x => x.Instances).Where(x => x != null);

            if (!stateExclude.IsNullOrEmpty())
                instances = instances.Where(x => x != null && !stateExclude.Contains(x.State.Name));
            
            return instances.Where(x => 
                (x.Tags?.Any(t => t?.Key?.ToLower() == "name" && t.Value == name) ?? false) == true || 
                x.InstanceId == name).ToArray();
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

        public static Task<DeleteTagsResponse> DeleteAllInstanceTags(this EC2Helper ec2, string instanceId, CancellationToken cancellationToken = default(CancellationToken))
            => ec2.DeleteTagsAsync(new List<string>() { instanceId }, tags: null, cancellationToken: cancellationToken);


        public static async Task<Instance> GetInstanceById(this EC2Helper ec2, string instanceId, bool throwIfNotFound = true, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (instanceId.IsNullOrEmpty())
                throw new ArgumentException("instanceId was not defined");

            var batch = await ec2.DescribeInstancesAsync(instanceIds: new List<string>() { instanceId }, filters: null, cancellationToken: cancellationToken);

            instanceId = instanceId?.Trim().ToLower();
            var instance = batch.SelectMany(x => x.Instances).FirstOrDefault(x => x?.InstanceId == instanceId);

            if (throwIfNotFound && instance == null)
                throw new Exception($"Instance {instanceId ?? "undefined"} was not found.");

            return instance;
        }

        public static async Task<Instance> GetInstanceByName(this EC2Helper ec2, string instanceName, bool throwIfNotFound = true, CancellationToken cancellationToken = default(CancellationToken))
        {
            instanceName = instanceName?.Trim().ToLower();

            if (instanceName.IsNullOrEmpty())
                throw new ArgumentException("instanceName was not defined");

            var batch = await ec2.DescribeInstancesAsync(cancellationToken: cancellationToken);
            var instance = batch.SelectMany(x => x.Instances)
                                .FirstOrDefault(x => !(x?.Tags).IsNullOrEmpty() && 
                                                x.State.Code != (int)InstanceStateCode.terminated &&
                                                x.State.Code != (int)InstanceStateCode.terminating &&
                                                x.Tags.Any(y => !(y?.Key).IsNullOrEmpty() && 
                                                           y.Key.ToLower().Trim() == "name" && y?.Value?.ToLower()?.Trim() == instanceName));

            if (throwIfNotFound && instance == null)
                throw new Exception($"Instance {instanceName ?? "undefined"} was not found or is being irreversibly terminated.");

            return instance;
        }

        /// <summary>
        /// Removes all tags then adds all tags specified in the dictionary
        /// </summary>
        public static async Task<bool> UpdateTagsAsync(this EC2Helper ec2, string instanceId, Dictionary<string, string> tags, CancellationToken cancellationToken = default(CancellationToken))
        {
            var instance = await ec2.GetInstanceById(instanceId);
            var deleteTags = await ec2.DeleteAllInstanceTags(instanceId);

            if (tags.IsNullOrEmpty())
            {
                instance = await ec2.GetInstanceById(instanceId);
                return instance.Tags.IsNullOrEmpty();
            }

            var createTags = await ec2.CreateTagsAsync(
                resourceIds: new List<string>() { instanceId },
                tags: tags);

            instance = await ec2.GetInstanceById(instanceId); 
            return instance.Tags.ToDictionary(x => x.Key, y => y.Value).CollectionEquals(tags, trim: true);
        }
    }
}
