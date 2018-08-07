using System.Threading.Tasks;
using System.Threading;
using Amazon.KeyManagementService.Model;
using System.Linq;
using System;
using AWSWrapper.IAM;
using System.Collections.Generic;
using AsmodatStandard.Threading;
using AsmodatStandard.Extensions.Collections;

namespace AWSWrapper.KMS
{
    public static class KMSHelperEx
    {
        public static async Task<AliasListEntry> GetKeyAliasByNameAsync(this KMSHelper kms, string name, StringComparison stringComparison = StringComparison.InvariantCultureIgnoreCase, bool throwIfNotFound = true, CancellationToken cancellationToken = default(CancellationToken))
        {
            var list = await kms.ListAliasesAsync(cancellationToken: cancellationToken);
            var alias = list.SingleOrDefault(x => 
            x.AliasName?.Equals($"alias/{name}", stringComparison) == true || 
            x.AliasName?.Equals(name, stringComparison) == true || 
            x.AliasArn?.Equals(name, stringComparison) == true || 
            x.TargetKeyId?.Equals(name, stringComparison) == true);

            if (alias == null && throwIfNotFound)
                throw new Exception($"Key alias '{name}' was not found.");

            return alias;
        }

        public static async Task<GrantListEntry[]> GetGrantsByKeyNameAsync(this KMSHelper kms, string keyName, StringComparison stringComparison = StringComparison.InvariantCultureIgnoreCase, bool throwIfNotFound = true, CancellationToken cancellationToken = default(CancellationToken))
        {
            var alias = await kms.GetKeyAliasByNameAsync(keyName, stringComparison, throwIfNotFound, cancellationToken);
            return await kms.ListGrantsAsync(alias.TargetKeyId, cancellationToken);
        }

        public static async Task<IEnumerable<GrantListEntry>> GetGrantsByKeyNameAsync(this KMSHelper kms, string keyName, string grantName, StringComparison stringComparison = StringComparison.InvariantCultureIgnoreCase, bool throwIfNotFound = true, CancellationToken cancellationToken = default(CancellationToken))
        {
            var alias = await kms.GetKeyAliasByNameAsync(keyName, stringComparison, throwIfNotFound, cancellationToken);

            if (alias == null && !throwIfNotFound)
                return null;

            var grants = await kms.ListGrantsAsync(alias.TargetKeyId, cancellationToken);
            return grants.Where(x => x.GrantId.Equals(grantName, stringComparison) || x.Name.Equals(grantName, stringComparison));
        }

        public static async Task<CreateGrantResponse> CreateRoleGrantByName(this KMSHelper kms, string keyName, string grantName, string roleName, KMSHelper.GrantType grant, StringComparison stringComparison = StringComparison.InvariantCultureIgnoreCase, bool throwIfNotFound = true, CancellationToken cancellationToken = default(CancellationToken))
        {
            var alias = await kms.GetKeyAliasByNameAsync(keyName, stringComparison, throwIfNotFound, cancellationToken);

            if (alias == null && !throwIfNotFound)
                return new CreateGrantResponse() { HttpStatusCode = System.Net.HttpStatusCode.NotFound };

            var role = await (new IAMHelper(kms._credentials)).GetRoleByNameAsync(roleName, StringComparison.InvariantCultureIgnoreCase, cancellationToken);
            return await kms.CreateGrantAsync(grantName, alias.TargetKeyId, role.Arn, grant, cancellationToken);
        }

        public static async Task<RetireGrantResponse[]> RemoveGrantsByName(this KMSHelper kms, string keyName, string grantName, StringComparison stringComparison = StringComparison.InvariantCultureIgnoreCase, bool throwIfNotFound = true, CancellationToken cancellationToken = default(CancellationToken))
        {
            var grants = await kms.GetGrantsByKeyNameAsync(keyName, grantName, stringComparison, throwIfNotFound: true, cancellationToken: cancellationToken);

            if (grants.IsNullOrEmpty() && throwIfNotFound)
                throw new Exception($"No grants with name '{grantName}' for key '{keyName}' were found.");

            return await grants.ForEachAsync(grant => kms.RetireGrantAsync(grant.KeyId, grant.GrantId));
        }
    }
}
