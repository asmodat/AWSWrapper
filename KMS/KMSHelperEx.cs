using System.Threading.Tasks;
using System.Threading;
using Amazon.KeyManagementService.Model;
using System.Linq;
using System;
using AWSWrapper.IAM;

namespace AWSWrapper.KMS
{
    public static class KMSHelperEx
    {
        public static async Task<AliasListEntry> GetKeyAliasByNameAsync(this KMSHelper kms, string name, StringComparison stringComparison = StringComparison.InvariantCultureIgnoreCase, CancellationToken cancellationToken = default(CancellationToken))
        {
            var list = await kms.ListAliasesAsync(cancellationToken: cancellationToken);
            var alias = list.Single(x => 
            x.AliasName?.Equals($"alias/{name}", stringComparison) == true || 
            x.AliasName?.Equals(name, stringComparison) == true || 
            x.AliasArn?.Equals(name, stringComparison) == true || 
            x.TargetKeyId?.Equals(name, stringComparison) == true);
            return alias;
        }

        public static async Task<GrantListEntry[]> GetGrantsByKeyNameAsync(this KMSHelper kms, string keyName, StringComparison stringComparison = StringComparison.InvariantCultureIgnoreCase, CancellationToken cancellationToken = default(CancellationToken))
        {
            var alias = await kms.GetKeyAliasByNameAsync(keyName, stringComparison, cancellationToken);
            return await kms.ListGrantsAsync(alias.TargetKeyId, cancellationToken);
        }

        public static async Task<GrantListEntry> GetGrantByKeyNameAsync(this KMSHelper kms, string keyName, string grantName, StringComparison stringComparison = StringComparison.InvariantCultureIgnoreCase, CancellationToken cancellationToken = default(CancellationToken))
        {
            var alias = await kms.GetKeyAliasByNameAsync(keyName, stringComparison, cancellationToken);
            var grants = await kms.ListGrantsAsync(alias.TargetKeyId, cancellationToken);
            return grants.Single(x => x.GrantId.Equals(grantName, stringComparison) || x.Name.Equals(grantName, stringComparison));
        }

        public static async Task<CreateGrantResponse> CreateRoleGrantByName(this KMSHelper kms, string keyName, string grantName, string roleName, KMSHelper.GrantType grant, StringComparison stringComparison = StringComparison.InvariantCultureIgnoreCase, CancellationToken cancellationToken = default(CancellationToken))
        {
            var alias = await kms.GetKeyAliasByNameAsync(keyName, stringComparison, cancellationToken);
            var role = await (new IAMHelper(kms._credentials)).GetRoleByNameAsync(roleName, StringComparison.InvariantCultureIgnoreCase, cancellationToken);
            return await kms.CreateGrantAsync(grantName, alias.TargetKeyId, role.Arn, grant, cancellationToken);
        }

        public static async Task<RetireGrantResponse> RemoveGrantByName(this KMSHelper kms, string keyName, string grantName, StringComparison stringComparison = StringComparison.InvariantCultureIgnoreCase, CancellationToken cancellationToken = default(CancellationToken))
        {
            var grant = await kms.GetGrantByKeyNameAsync(keyName, grantName, stringComparison, cancellationToken);
            return await kms.RetireGrantAsync(grant.KeyId, grant.GrantId, cancellationToken);
        }
    }
}
