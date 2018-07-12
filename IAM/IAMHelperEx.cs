using System.Threading.Tasks;
using System.Threading;
using Amazon.IdentityManagement.Model;
using AsmodatStandard.Extensions.Collections;
using System.Linq;
using System.Collections.Generic;
using System;
using AsmodatStandard.Threading;
using AsmodatStandard.Extensions;
using AWSWrapper.S3;

namespace AWSWrapper.IAM
{
    public static class IAMHelperEx
    {
        public static async Task<AccessKeyMetadata> GetAccessKeyByIdAsync(this IAMHelper iam, string name, StringComparison stringComparison = StringComparison.InvariantCultureIgnoreCase, CancellationToken cancellationToken = default(CancellationToken))
        {
            var list = await iam.ListAccessKeysAsync(cancellationToken: cancellationToken);
            return list.Single(x => x.AccessKeyId.Equals(name, stringComparison));
        }

        public static Task<CreatePolicyResponse> CreatePolicyS3Async(this IAMHelper iam, IEnumerable<string> paths, string name, IEnumerable<S3Helper.Permissions> permissions, string description = null, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (permissions == null)
                throw new ArgumentNullException($"{nameof(permissions)} can't be null");

            var actions = permissions.Any(p => p == S3Helper.Permissions.All) ?
                "\"s3:*\"" : permissions.SelectMany(
                p => p.ToStringFlagArray().Select(s => $"s3:{s}"))
                .Distinct().JsonSerialize();

            var sub_policies = "";

            paths.ForEach((path, i) => {
                sub_policies +=
$@"
        {{
            ""Sid"": ""AWSHelper{i}"",
            ""Effect"": ""Allow"",
            ""Action"": {(actions.IsNullOrEmpty() ? "[ ]" : actions)},
            ""Resource"": ""arn:aws:s3:::{path}""
        }},";
            });

            //this must be present otherwise its not possible to delete objects from the buckt due to some AWS bug
            sub_policies += $"{{ \"Sid\": \"AWSHelper{paths.Count()}\", \"Effect\": \"Allow\", \"Action\": [ \"s3:List*\" ], \"Resource\": \"arn:aws:s3:::*\" }}";
            //sub_policies = sub_policies.TrimEnd(',');

            string json =
$@"{{
    ""Version"": ""2012-10-17"",
    ""Statement"": [{sub_policies}
    ]
}}";
            return iam.CreatePolicyAsync(name: name, description: description, json: json, cancellationToken: cancellationToken);
        }

        public static async Task<ManagedPolicy> GetPolicyByNameAsync(this IAMHelper iam, string name, StringComparison stringComparison = StringComparison.InvariantCultureIgnoreCase, CancellationToken cancellationToken = default(CancellationToken))
        {
            var list = await iam.ListPoliciesAsync(cancellationToken: cancellationToken);
            return list.Single(x => x.Arn.Equals(name, stringComparison) || x.PolicyName.Equals(name, stringComparison));
        }

        public static async Task<ManagedPolicy[]> GetPoliciesByNamesAsync(this IAMHelper iam, IEnumerable<string> names, bool onlyAttached = false, StringComparison stringComparison = StringComparison.InvariantCultureIgnoreCase, CancellationToken cancellationToken = default(CancellationToken))
        {
            var list = await iam.ListPoliciesAsync(onlyAttached: onlyAttached, cancellationToken: cancellationToken);

            var results = new List<ManagedPolicy>();

            foreach(var name in names)
                results.Add(list.Single(x => x.Arn.Equals(name, stringComparison) || x.PolicyName.Equals(name, stringComparison)));

            return results.ToArray();
        }

        public static async Task<Role> GetRoleByNameAsync(this IAMHelper iam, string name, StringComparison stringComparison = StringComparison.InvariantCultureIgnoreCase, CancellationToken cancellationToken = default(CancellationToken))
        {
            var list = await iam.ListRolesAsync(cancellationToken: cancellationToken);
            return list.Single(x => x.RoleName.Equals(name, stringComparison));
        }

        public static async Task<DeletePolicyResponse> DeletePolicyByNameAsync(this IAMHelper iam, string name, StringComparison stringComparison = StringComparison.InvariantCultureIgnoreCase, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (name.IsNullOrEmpty())
                throw new ArgumentException($"{nameof(name)} is null or empty");

            var policy = await iam.GetPolicyByNameAsync(name: name, stringComparison: stringComparison, cancellationToken: cancellationToken);

            var versions = (await iam.ListPolicyVersionsAsync(policy.Arn, cancellationToken)).Where(v => !v.IsDefaultVersion);

            await versions.ForEachAsync(
                v => iam.DeletePolicyVersionAsync(arn: policy.Arn, versionId: v.VersionId,  cancellationToken: cancellationToken), 
                maxDegreeOfParallelism: iam._maxDegreeOfParalelism);

            return await iam.DeletePolicyAsync(policy.Arn, cancellationToken);
        }

        public static async Task<(DeleteRoleResponse role, DetachRolePolicyResponse[] policies)> DeleteRoleAsync(this IAMHelper iam, string roleName, bool detachPolicies, StringComparison stringComparison = StringComparison.InvariantCultureIgnoreCase, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (roleName.IsNullOrEmpty())
                throw new ArgumentException($"{nameof(roleName)} is null or empty");

            DetachRolePolicyResponse[] detachRolePolicyResponses = null;
            if (detachPolicies)
            {
                var targets = await iam.ListAttachedRolePoliciesAsync(roleName, cancellationToken: cancellationToken);

                detachRolePolicyResponses = await targets.ForEachAsync(policy => iam.DetachRolePolicyAsync(roleName, policy.PolicyArn, cancellationToken)
                , maxDegreeOfParallelism: iam._maxDegreeOfParalelism);
            }

            return (await iam.DeleteRoleAsync(roleName, cancellationToken), detachRolePolicyResponses);
        }

        public static async Task<(CreateRoleResponse roleResponse, AttachRolePolicyResponse[] policiesResponse)> CreateRoleWithPoliciesAsync(
            this IAMHelper iam, string roleName,
            string[] policies,
            string roleDescription = null,
            StringComparison stringComparison = StringComparison.InvariantCultureIgnoreCase,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var policyDoc = $@"{{""Version"":""2012-10-17"",""Statement"":[{{""Effect"":""Allow"",""Principal"":{{""Service"":[""ec2.amazonaws.com"",""ecs-tasks.amazonaws.com""]}},""Action"":[""sts:AssumeRole""]}}]}}";

            var tR = iam.CreateRoleAsync(roleName: roleName, description: roleDescription, path: null, maxSessionDuration: 12 * 3600, assumeRolePolicyDocument: policyDoc, cancellationToken: cancellationToken);
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
