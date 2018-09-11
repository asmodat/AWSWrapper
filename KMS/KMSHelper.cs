using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amazon.KeyManagementService;
using Amazon.KeyManagementService.Model;
using AWSWrapper.Extensions;
using AsmodatStandard.Extensions;
using AsmodatStandard.Extensions.Collections;
using Amazon.SecurityToken.Model;

namespace AWSWrapper.KMS
{
    public partial class KMSHelper
    {
        public enum GrantType
        {
            None = 0,
            Encrypt = 1,
            Decrypt = 1 << 1,
            Retire = 1 << 2,
            Create = 1 << 3,
            Describe = 1 << 4,
            EncryptDecrypt = Encrypt | Decrypt,
            All = EncryptDecrypt | Describe | Create | Retire
        }

        internal readonly int _maxDegreeOfParalelism;
        internal readonly AmazonKeyManagementServiceClient _KMSClient;
        internal readonly Credentials _credentials = null;

        public KMSHelper(Credentials credentials, int maxDegreeOfParalelism = 2)
        {
            _maxDegreeOfParalelism = maxDegreeOfParalelism;
            _credentials = credentials;

            if (credentials != null)
            {
                _KMSClient = new AmazonKeyManagementServiceClient(credentials);
            }
            else
            {
                _KMSClient = new AmazonKeyManagementServiceClient();
            }
        }

        public Task<CreateGrantResponse> CreateGrantAsync(
            string grantName,
            string keyId,
            string principalARN,
            GrantType grant,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var opList = new List<string>();

            if ((grant & GrantType.Encrypt) != 0)
            {
                opList.Add(GrantOperation.Encrypt);
                opList.Add(GrantOperation.ReEncryptFrom);
                opList.Add(GrantOperation.ReEncryptTo);
                opList.Add(GrantOperation.GenerateDataKey);
                opList.Add(GrantOperation.GenerateDataKeyWithoutPlaintext);
            }

            if ((grant & GrantType.Decrypt) != 0)
                opList.Add(GrantOperation.Decrypt);

            if ((grant & GrantType.Retire) != 0)
                opList.Add(GrantOperation.RetireGrant);

            if ((grant & GrantType.Describe) != 0)
                opList.Add(GrantOperation.DescribeKey);

            return _KMSClient.CreateGrantAsync(
                new CreateGrantRequest()
                {
                    KeyId = keyId,
                    GranteePrincipal = principalARN,
                    Name = grantName,
                    Operations = opList,
                },
                cancellationToken).EnsureSuccessAsync();
        }

        public Task<RetireGrantResponse> RetireGrantAsync(
            string keyId,
            string grantId,
            CancellationToken cancellationToken = default(CancellationToken))
            => _KMSClient.RetireGrantAsync(
                new RetireGrantRequest()
                {
                    KeyId = keyId,
                    GrantId = grantId
                },
                cancellationToken).EnsureSuccessAsync();

        public Task<PutKeyPolicyResponse> PutKeyPolicyAsync(
            string keyId,
            string policyName,
            string policy,
            bool bypassPolicyLockoutSafetyCheck = false,
            CancellationToken cancellationToken = default(CancellationToken))
            => _KMSClient.PutKeyPolicyAsync(
                new PutKeyPolicyRequest() {
                    KeyId = keyId,
                    Policy = policy,
                    PolicyName = policyName,
                    BypassPolicyLockoutSafetyCheck = bypassPolicyLockoutSafetyCheck
                },
                cancellationToken).EnsureSuccessAsync();

        public Task<GetKeyPolicyResponse> GetKeyPolicyAsync(
        string keyId,
        string policyName,
        CancellationToken cancellationToken = default(CancellationToken))
        => _KMSClient.GetKeyPolicyAsync(
            new GetKeyPolicyRequest() {
                    KeyId = keyId,
                    PolicyName = policyName
            },
            cancellationToken).EnsureSuccessAsync();

        public async Task<string[]> ListKeyPoliciesAsync(string keyId, CancellationToken cancellationToken = default(CancellationToken))
        {
            ListKeyPoliciesResponse response = null;
            var results = new List<string>();
            while ((response = await _KMSClient.ListKeyPoliciesAsync(new ListKeyPoliciesRequest()
            {
                Marker = response?.NextMarker,
                Limit = 1000,
                KeyId = keyId
            }, cancellationToken).EnsureSuccessAsync()) != null)
            {
                if (!response.PolicyNames.IsNullOrEmpty())
                    results.AddRange(response.PolicyNames);

                if (!response.Truncated)
                    break;

                await Task.Delay(100);
            }

            return results.ToArray();
        }
        
        public async Task<KeyListEntry[]> ListKeysAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            ListKeysResponse response = null;
            var results = new List<KeyListEntry>();
            while ((response = await _KMSClient.ListKeysAsync(new ListKeysRequest()
            {
                Marker = response?.NextMarker,
                Limit = 1000
            }, cancellationToken).EnsureSuccessAsync()) != null)
            {
                if (!response.Keys.IsNullOrEmpty())
                    results.AddRange(response.Keys);
                
                if (!response.Truncated)
                    break;

                await Task.Delay(100);
            }

            return results.ToArray();
        }

        public async Task<AliasListEntry[]> ListAliasesAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            ListAliasesResponse response = null;
            var results = new List<AliasListEntry>();
            while ((response = await _KMSClient.ListAliasesAsync(new ListAliasesRequest()
            {
                Marker = response?.NextMarker,
                Limit = 1000
            }, cancellationToken).EnsureSuccessAsync()) != null)
            {
                if (!response.Aliases.IsNullOrEmpty())
                    results.AddRange(response.Aliases);

                if (!response.Truncated)
                    break;

                await Task.Delay(100);
            }

            return results.ToArray();
        }

        public async Task<GrantListEntry[]> ListGrantsAsync(string keyId, CancellationToken cancellationToken = default(CancellationToken))
        {
            ListGrantsResponse response = null;
            var results = new List<GrantListEntry>();
            while ((response = await _KMSClient.ListGrantsAsync(new ListGrantsRequest()
            {
                Marker = response?.NextMarker,
                Limit = 1000,
                KeyId = keyId
            }, cancellationToken).EnsureSuccessAsync()) != null)
            {
                if (!response.Grants.IsNullOrEmpty())
                    results.AddRange(response.Grants);

                if (!response.Truncated)
                    break;

                await Task.Delay(100);
            }

            return results.ToArray();
        }
    }
}
