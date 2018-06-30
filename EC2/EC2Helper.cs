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
        internal readonly AmazonEC2Client _EC2Client;

        public EC2Helper(int maxDegreeOfParalelism = 8)
        {
            _maxDegreeOfParalelism = maxDegreeOfParalelism;
            _EC2Client = new AmazonEC2Client();
        }

        public async Task<Reservation[]> DescribeInstancesAsync(List<string> instanceIds = null, Dictionary<string, List<string>> filters = null, CancellationToken cancellationToken = default(CancellationToken))
        {
            var filterList = filters?.Select(x => new Filter(x.Key, x.Value)).ToList();

            string nextToken = null;
            DescribeInstancesResponse response;
            var results = new List<Reservation>();
            while ((response = await _EC2Client.DescribeInstancesAsync(new DescribeInstancesRequest()
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

        public Task<StartInstancesResponse> StartInstancesAsync(List<string> instanceIds, string additionalInfo = null, CancellationToken cancellationToken = default(CancellationToken))
            => _EC2Client.StartInstancesAsync(new StartInstancesRequest() {
                InstanceIds = instanceIds,
                AdditionalInfo = additionalInfo
            }, cancellationToken).EnsureSuccessAsync();

        public Task<StopInstancesResponse> StopInstancesAsync(List<string> instanceIds, bool force = false, CancellationToken cancellationToken = default(CancellationToken))
            => _EC2Client.StopInstancesAsync(new StopInstancesRequest()
            {
                InstanceIds = instanceIds,
                Force = force,
            }, cancellationToken).EnsureSuccessAsync();

        public Task<TerminateInstancesResponse> TerminateInstancesAsync(List<string> instanceIds, CancellationToken cancellationToken = default(CancellationToken))
            => _EC2Client.TerminateInstancesAsync(new TerminateInstancesRequest()
            {
                InstanceIds = instanceIds
            }, cancellationToken).EnsureSuccessAsync();
    }
}
