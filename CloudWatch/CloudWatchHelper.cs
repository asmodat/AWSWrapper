using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon;
using Amazon.CloudWatch;
using Amazon.CloudWatchLogs;
using AsmodatStandard.Extensions;
using AsmodatStandard.Threading;
using AsmodatStandard.Extensions.Collections;
using AWSWrapper.Extensions;
using System.Threading;
using Amazon.CloudWatchLogs.Model;
using System.Net;

namespace AWSWrapper.CloudWatch
{
    public partial class CloudWatchHelper
    {
        private readonly int _maxDegreeOfParalelism;
        private readonly AmazonCloudWatchClient _client;
        private readonly AmazonCloudWatchLogsClient _clientLogs;

        public CloudWatchHelper(int maxDegreeOfParalelism = 8)
        {
            _maxDegreeOfParalelism = maxDegreeOfParalelism;
            _client = new AmazonCloudWatchClient();
            _clientLogs = new AmazonCloudWatchLogsClient();
        }

        public Task CreateLogGroupAsync(string name, CancellationToken cancellationToken = default(CancellationToken))
            => _clientLogs.CreateLogGroupAsync(new CreateLogGroupRequest()
            {
                LogGroupName = name,
                Tags = new Dictionary<string, string>() { { "Timestamp", DateTime.UtcNow.ToRfc3339String() } }
            }, cancellationToken).EnsureSuccessAsync();

        public async Task<DeleteLogGroupResponse> DeleteLogGroupAsync(string name, bool throwIfNotFound = true, CancellationToken cancellationToken = default(CancellationToken))
        {
            try
            {
                var result = (throwIfNotFound) ?
                    _clientLogs.DeleteLogGroupAsync(new DeleteLogGroupRequest() { LogGroupName = name }, cancellationToken) :
                    _clientLogs.DeleteLogGroupAsync(new DeleteLogGroupRequest() { LogGroupName = name });

                return await result.EnsureSuccessAsync();
            }
            catch (ResourceNotFoundException ex)
            {
                if (throwIfNotFound)
                    throw ex;

                return new DeleteLogGroupResponse() { HttpStatusCode = HttpStatusCode.NotFound };
            }
        }

        public Task<DeleteLogGroupResponse[]> DeleteLogGroupsAsync(IEnumerable<string> names, bool throwIfNotFound = true, CancellationToken cancellationToken = default(CancellationToken))
            => names.ForEachAsync(name => DeleteLogGroupAsync(name, throwIfNotFound: throwIfNotFound, cancellationToken: cancellationToken), _maxDegreeOfParalelism, cancellationToken)
            .EnsureSuccess();
    }
}
