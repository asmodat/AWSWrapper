using System.Threading.Tasks;
using System.Threading;
using Amazon.IdentityManagement.Model;
using AsmodatStandard.Extensions.Collections;
using System.Linq;
using System.Collections.Generic;
using System;
using AsmodatStandard.Threading;

namespace AWSWrapper.IAM
{
    public static class IAMHelperEx
    {
        public static Task<CreatePolicyResponse> CreateAdminAccessPolicyS3Async(this IAMHelper iam, string path, string name, string description = null, CancellationToken cancellationToken = default(CancellationToken))
        {
            string json =
$@"{{
    ""Version"": ""{DateTime.UtcNow.ToShortDateString()}"",
    ""Statement"": [
        {{
            ""Sid"": ""VisualEditor0"",
            ""Effect"": ""Allow"",
            ""Action"": [
                ""s3:ListAllMyBuckets"",
                ""s3:HeadBucket""
            ],
            ""Resource"": ""*""
        }},
        {{
            ""Sid"": ""VisualEditor1"",
            ""Effect"": ""Allow"",
            ""Action"": ""s3:*"",
            ""Resource"": ""arn:aws:s3:::{path}""
        }}
    ]
}}";
            return iam.CreatePolicyAsync(name: name, description: description, json: json, cancellationToken: cancellationToken);
        }

        public static async Task<ManagedPolicy> GetPolicyByNameAsync(this IAMHelper iam, string name, StringComparison stringComparison = StringComparison.InvariantCultureIgnoreCase, CancellationToken cancellationToken = default(CancellationToken))
        {
            var list = await iam.ListPoliciesAsync(cancellationToken: cancellationToken);
            return list.Single(x => x.PolicyName.Equals(name, stringComparison));
        }

        public static async Task<Role> GetRoleByNameAsync(this IAMHelper iam, string name, StringComparison stringComparison = StringComparison.InvariantCultureIgnoreCase, CancellationToken cancellationToken = default(CancellationToken))
        {
            var list = await iam.ListRolesAsync(cancellationToken: cancellationToken);
            return list.Single(x => x.RoleName.Equals(name, stringComparison));
        }

        public static async Task<DeletePolicyResponse> DeletePolicyByNameAsync(this IAMHelper iam, string name, StringComparison stringComparison = StringComparison.InvariantCultureIgnoreCase, CancellationToken cancellationToken = default(CancellationToken))
        {
            var policy = await iam.GetPolicyByNameAsync(name: name, stringComparison: stringComparison, cancellationToken: cancellationToken);
            return await iam.DeletePolicyAsync(arn: policy.Arn, cancellationToken: cancellationToken);
        }

        public static async Task<(CreateRoleResponse roleResponse, AttachRolePolicyResponse[] policiesResponse)> CreateRoleWithPoliciesAsync(
            this IAMHelper iam, string roleName,
            string[] policies,
            string roleDescription = null,
            StringComparison stringComparison = StringComparison.InvariantCultureIgnoreCase,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var tR = iam.CreateRoleAsync(roleName: roleName, description: roleDescription, path: null, maxSessionDuration: 12, assumeRolePolicyDocument: null, cancellationToken: cancellationToken);
            var list = await iam.ListPoliciesAsync(cancellationToken: cancellationToken);
            var mp = new ManagedPolicy[policies.Length];

            for (int i = 0; i < policies.Length; i++)
            {
                var policy = policies[i];
                mp[i] = list.Single(x => x.PolicyName.Equals(policy, stringComparison) || x.PolicyName.Equals(policy, stringComparison));
            }

            var roleResponse = await tR;
            var prs = await mp.ForEachAsync(p => iam.AttachRolePolicyAsync(roleResponse.Role.RoleName, p.Arn, cancellationToken)
            , iam._maxDegreeOfParalelism, cancellationToken: cancellationToken);

            return (roleResponse, prs);
        }
    }
}
