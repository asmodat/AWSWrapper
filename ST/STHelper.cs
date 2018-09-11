using System;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
using AWSWrapper.Extensions;

namespace AWSWrapper.ST
{
    public partial class STHelper
    {
        internal readonly int _maxDegreeOfParalelism;
        internal readonly AmazonSecurityTokenServiceClient _STClient;

        public STHelper(Credentials credentials, int maxDegreeOfParalelism = 2)
        {
            _maxDegreeOfParalelism = maxDegreeOfParalelism;

            if (credentials != null)
                _STClient = new AmazonSecurityTokenServiceClient(credentials);
            else
                _STClient = new AmazonSecurityTokenServiceClient();
        }

        public Task<AssumeRoleResponse> AssumeRoleAsync(
            string roleArn,
            int duration = 3600,
            string roleSessionName = null,
            CancellationToken cancellationToken = default(CancellationToken)) =>
            _STClient.AssumeRoleAsync(
                 new AssumeRoleRequest()
                 {
                     DurationSeconds = duration,
                     RoleArn = roleArn,
                     RoleSessionName = roleSessionName ?? $"AWSHelper-{Guid.NewGuid().ToString()}",
                 }, cancellationToken).EnsureSuccessAsync();

        public Task<GetCallerIdentityResponse> GetCallerIdentityAsync(CancellationToken cancellationToken = default(CancellationToken)) =>
           _STClient.GetCallerIdentityAsync(new GetCallerIdentityRequest(), cancellationToken).EnsureSuccessAsync();
    }
}
