using System;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SecretsManager;
using Amazon.SecretsManager.Model;
using AWSWrapper.Extensions;
using System.IO;
using AsmodatStandard.Extensions;
using Amazon.SecurityToken.Model;
using Amazon.Runtime;

namespace AWSWrapper.SM
{
    public partial class SMHelper
    {
        private readonly int _maxDegreeOfParalelism;
        internal readonly AmazonSecretsManagerClient _client;

        public SMHelper(AWSCredentials credentials = null, string region = null, int maxDegreeOfParalelism = 2)
        {
            _maxDegreeOfParalelism = maxDegreeOfParalelism;

            if (credentials != null)
            {
                _client = region == null ?
                    new AmazonSecretsManagerClient(credentials) : new
                    AmazonSecretsManagerClient(credentials, region: Amazon.RegionEndpoint.GetBySystemName(region));
            }
            else
            {
                _client = region == null ?
                    new AmazonSecretsManagerClient() :
                    new AmazonSecretsManagerClient(region: Amazon.RegionEndpoint.GetBySystemName(region));
            }
        }

        public async Task<string> GetSecret(string name, string versionStage = "AWSCURRENT", CancellationToken cancellationToken = default(CancellationToken))
        {
            var response = await _client.GetSecretValueAsync(new GetSecretValueRequest()
            {
                SecretId = name,
                VersionStage = versionStage
            }).EnsureSuccessAsync();

            if (response.SecretString != null)
                return response.SecretString;

            using (var sr = new StreamReader(response.SecretBinary))
                return System.Text.Encoding.UTF8.GetString(Convert.FromBase64String(sr.ReadToEnd()));
        }
    }
}
