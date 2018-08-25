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

namespace AWSWrapper.EC2
{
    public partial class EC2Helper
    {
        private readonly int _maxDegreeOfParalelism;
        internal readonly AmazonEC2Client _client;

        public enum AMIStatus
        {
            available = 1,
            pending = 1 << 1,
            failed = 1 << 2
        }

        public enum InstanceStateCode
        {
            pending = 0,
            running = 16,
            terminating = 32,
            terminated = 48,
            stopping = 64,
            stopped = 80
        }

        public enum InstanceModel
        {
            T2Nano = 1,
            T2Micro,
            T2Small,
            T2Medium,
            T2Large,
            T2XLarge,
            T22XLarge
        }

        public EC2Helper(int maxDegreeOfParalelism = 8)
        {
            _maxDegreeOfParalelism = maxDegreeOfParalelism;
            _client = new AmazonEC2Client();
        }

        public async Task<Reservation[]> DescribeInstancesAsync(List<string> instanceIds = null, Dictionary<string, List<string>> filters = null, CancellationToken cancellationToken = default(CancellationToken))
        {
            var filterList = filters?.Select(x => new Filter(x.Key, x.Value)).ToList();

            string nextToken = null;
            DescribeInstancesResponse response;
            var results = new List<Reservation>();
            while ((response = await _client.DescribeInstancesAsync(new DescribeInstancesRequest()
            {
                MaxResults = 1000,
                NextToken = nextToken,
                Filters = filterList,
                InstanceIds = instanceIds

            }, cancellationToken).EnsureSuccessAsync()) != null)
            {
                if ((response.Reservations?.Count ?? 0) == 0)
                    break;

                results.AddRange(response.Reservations);

                if (response.NextToken.IsNullOrEmpty())
                    break;

                nextToken = response.NextToken;
            }

            return results.ToArray();
        }

        public Task<RunInstancesResponse> CreateInstanceAsync(
            string imageId,
            InstanceType instanceType,
            string keyName,
            string securityGroupId,
            string subnetId,
            string roleName,
            ShutdownBehavior shutdownBehavior,
            bool associatePublicIpAddress,
            Dictionary<string, string> tags,
            CancellationToken cancellationToken = default(CancellationToken))
            => _client.RunInstancesAsync(new RunInstancesRequest()
            {
                ImageId = imageId,
                InstanceType = instanceType,
                MinCount = 1,
                MaxCount = 1,
                KeyName = keyName,
                InstanceInitiatedShutdownBehavior = shutdownBehavior,
                DisableApiTermination = false,
                IamInstanceProfile = roleName.IsNullOrEmpty() ? null : new IamInstanceProfileSpecification() {
                    Name = roleName
                },
                NetworkInterfaces = new List<InstanceNetworkInterfaceSpecification>()
                {
                    new InstanceNetworkInterfaceSpecification()
                    {
                        DeviceIndex = 0,
                        SubnetId = subnetId,
                        Groups = new List<string>() { securityGroupId },
                        AssociatePublicIpAddress = associatePublicIpAddress,
                        Description = "Primary network interface",
                        DeleteOnTermination = true,
                    }
                },
                TagSpecifications = new List<TagSpecification>()
                {
                    new TagSpecification(){
                        ResourceType =  ResourceType.Instance,
                        Tags = tags.Select(x => new Tag(){ Key = x.Key, Value = x.Value }).ToList()
                    }
                },
            }, cancellationToken).EnsureSuccessAsync();


        public async Task<InstanceStatus> DescribeInstanceStatusAsync(string instanceId, CancellationToken cancellationToken = default(CancellationToken))
            => (await _client.DescribeInstanceStatusAsync(new DescribeInstanceStatusRequest()
            {
                InstanceIds = new List<string>() { instanceId },
                IncludeAllInstances = true
            }, cancellationToken).EnsureSuccessAsync()).InstanceStatuses.SingleOrDefault();

        public async Task<Image> DescribeImageAsync(string name, AMIStatus state = AMIStatus.available, CancellationToken cancellationToken = default(CancellationToken))
            => (await _client.DescribeImagesAsync(new DescribeImagesRequest()
            {
                Filters = new List<Filter>() {
                    new Filter() { Name = "name", Values = new List<string>() { name } },
                    new Filter() { Name = "state", Values = new List<string>() { state.ToString() } } }
            }, cancellationToken).EnsureSuccessAsync()).Images.SingleOrDefault();

        public Task<StartInstancesResponse> StartInstancesAsync(List<string> instanceIds, string additionalInfo = null, CancellationToken cancellationToken = default(CancellationToken))
            => _client.StartInstancesAsync(new StartInstancesRequest() {
                InstanceIds = instanceIds,
                AdditionalInfo = additionalInfo
            }, cancellationToken).EnsureSuccessAsync();

        public Task<StopInstancesResponse> StopInstancesAsync(List<string> instanceIds, bool force = false, CancellationToken cancellationToken = default(CancellationToken))
            => _client.StopInstancesAsync(new StopInstancesRequest()
            {
                InstanceIds = instanceIds,
                Force = force,
            }, cancellationToken).EnsureSuccessAsync();

        public Task<TerminateInstancesResponse> TerminateInstancesAsync(List<string> instanceIds, CancellationToken cancellationToken = default(CancellationToken))
            => _client.TerminateInstancesAsync(new TerminateInstancesRequest()
            {
                InstanceIds = instanceIds
            }, cancellationToken).EnsureSuccessAsync();
    }
}
