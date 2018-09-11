using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.IdentityManagement;
using Amazon.IdentityManagement.Model;
using AWSWrapper.Extensions;
using AsmodatStandard.Extensions;
using AsmodatStandard.Extensions.Collections;
using Amazon.SecurityToken.Model;

namespace AWSWrapper.IAM
{
    public partial class IAMHelper
    {
        internal readonly int _maxDegreeOfParalelism;
        internal readonly AmazonIdentityManagementServiceClient _IAMClient;

        public IAMHelper(Credentials credentials, int maxDegreeOfParalelism = 2)
        {
            _maxDegreeOfParalelism = maxDegreeOfParalelism;

            if (credentials != null)
                _IAMClient = new AmazonIdentityManagementServiceClient(credentials);
            else
                _IAMClient = new AmazonIdentityManagementServiceClient();
        }

        public Task<DeleteRoleResponse> DeleteRoleAsync(
            string roleName,
            CancellationToken cancellationToken = default(CancellationToken))
            => _IAMClient.DeleteRoleAsync(
                new DeleteRoleRequest() { RoleName = roleName  },
                cancellationToken).EnsureSuccessAsync();

        public Task<CreateRoleResponse> CreateRoleAsync(
            string roleName, 
            string description,
            string assumeRolePolicyDocument,
            string path = null, 
            int maxSessionDuration = 12*3600,
            CancellationToken cancellationToken = default(CancellationToken))
            => _IAMClient.CreateRoleAsync(
                new CreateRoleRequest() {
                    Description = description,
                    RoleName = roleName,
                    MaxSessionDuration = maxSessionDuration,
                    AssumeRolePolicyDocument = assumeRolePolicyDocument, Path = path },
                cancellationToken).EnsureSuccessAsync();

        public Task<CreateInstanceProfileResponse> CreateInstanceProfileAsync(
            string name,
            CancellationToken cancellationToken = default(CancellationToken))
            => _IAMClient.CreateInstanceProfileAsync(
                new CreateInstanceProfileRequest()
                {
                    InstanceProfileName = name
                },
                cancellationToken).EnsureSuccessAsync();

        public Task<DeleteInstanceProfileResponse> DeleteInstanceProfileAsync(
            string name,
            CancellationToken cancellationToken = default(CancellationToken))
            => _IAMClient.DeleteInstanceProfileAsync(
                new DeleteInstanceProfileRequest() { InstanceProfileName = name, },
                cancellationToken).EnsureSuccessAsync();

        public Task<RemoveRoleFromInstanceProfileResponse> RemoveRoleFromInstanceProfileAsync(
           string profileName,
           string roleName,
           CancellationToken cancellationToken = default(CancellationToken))
           => _IAMClient.RemoveRoleFromInstanceProfileAsync(
               new RemoveRoleFromInstanceProfileRequest() {
                   InstanceProfileName = profileName,
                   RoleName = roleName
               },
               cancellationToken).EnsureSuccessAsync();

        public Task<AddRoleToInstanceProfileResponse> AddRoleToInstanceProfileAsync(
            string profileName,
            string roleName,
            CancellationToken cancellationToken = default(CancellationToken))
            => _IAMClient.AddRoleToInstanceProfileAsync(
                new AddRoleToInstanceProfileRequest()
                {
                    InstanceProfileName = profileName,
                    RoleName = roleName
                },
                cancellationToken).EnsureSuccessAsync();

        public Task<AttachRolePolicyResponse> AttachRolePolicyAsync(
            string roleName,
            string policyArn,
            CancellationToken cancellationToken = default(CancellationToken))
            => _IAMClient.AttachRolePolicyAsync(
                new AttachRolePolicyRequest() { RoleName = roleName, PolicyArn = policyArn },
                cancellationToken).EnsureSuccessAsync();

        public Task<CreatePolicyResponse> CreatePolicyAsync(string name, string description, string json, string path = null, CancellationToken cancellationToken = default(CancellationToken))
            => _IAMClient.CreatePolicyAsync(
                new CreatePolicyRequest() { Description = description, PolicyName = name, PolicyDocument = json, Path = path },
                cancellationToken).EnsureSuccessAsync();

        public Task<DeletePolicyResponse> DeletePolicyAsync(string arn, CancellationToken cancellationToken = default(CancellationToken))
            => _IAMClient.DeletePolicyAsync(
                new DeletePolicyRequest() {
                    PolicyArn = arn
                },
                cancellationToken).EnsureSuccessAsync();

        public Task<DeletePolicyVersionResponse> DeletePolicyVersionAsync(string arn, string versionId, CancellationToken cancellationToken = default(CancellationToken))
            => _IAMClient.DeletePolicyVersionAsync(
                new DeletePolicyVersionRequest()
                {
                    PolicyArn = arn,
                    VersionId = versionId
                },
                cancellationToken).EnsureSuccessAsync();

        public async Task<ManagedPolicy[]> ListPoliciesAsync(string pathPrefx = null, bool onlyAttached = false, CancellationToken cancellationToken = default(CancellationToken))
        {
            ListPoliciesResponse response = null;
            var results = new List<ManagedPolicy>();
            while ((response = await _IAMClient.ListPoliciesAsync(new ListPoliciesRequest()
            {
                MaxItems = 1000,
                Scope = PolicyScopeType.All,
                OnlyAttached = onlyAttached,
                Marker = response?.Marker,
                PathPrefix = pathPrefx
            }, cancellationToken).EnsureSuccessAsync()) != null)
            {
                if(!response.Policies.IsNullOrEmpty())
                    results.AddRange(response.Policies);

                if (!response.IsTruncated)
                    break;

                await Task.Delay(100);
            }

            return results.ToArray();
        }

        public async Task<InstanceProfile[]> ListInstanceProfilesAsync(string pathPrefx = null, CancellationToken cancellationToken = default(CancellationToken))
        {
            ListInstanceProfilesResponse response = null;
            var results = new List<InstanceProfile>();
            while ((response = await _IAMClient.ListInstanceProfilesAsync(new ListInstanceProfilesRequest()
            {
                MaxItems = 100,
                Marker = response?.Marker,
                PathPrefix = pathPrefx,
            }, cancellationToken).EnsureSuccessAsync()) != null)
            {
                if (!response.InstanceProfiles.IsNullOrEmpty())
                    results.AddRange(response.InstanceProfiles);

                if (!response.IsTruncated)
                    break;

                await Task.Delay(100);
            }

            return results.ToArray();
        }

        public async Task<InstanceProfile[]> ListInstanceProfilesForRoleAsync(string roleName, CancellationToken cancellationToken = default(CancellationToken))
        {
            ListInstanceProfilesForRoleResponse response = null;
            var results = new List<InstanceProfile>();
            while ((response = await _IAMClient.ListInstanceProfilesForRoleAsync(new ListInstanceProfilesForRoleRequest()
            {
                MaxItems = 100,
                Marker = response?.Marker,
                RoleName = roleName
            }, cancellationToken).EnsureSuccessAsync()) != null)
            {
                if (!response.InstanceProfiles.IsNullOrEmpty())
                    results.AddRange(response.InstanceProfiles);

                if (!response.IsTruncated)
                    break;

                await Task.Delay(100);
            }

            return results.ToArray();
        }

        public async Task<string[]> ListRolePoliciesAsync(string roleName, CancellationToken cancellationToken = default(CancellationToken))
        {
            ListRolePoliciesResponse response = null;
            var results = new List<string>();
            while ((response = await _IAMClient.ListRolePoliciesAsync(new ListRolePoliciesRequest()
            {
                MaxItems = 1000,
                Marker = response?.Marker,
                RoleName = roleName
            }, cancellationToken).EnsureSuccessAsync()) != null)
            {
                if (!response.PolicyNames.IsNullOrEmpty())
                    results.AddRange(response.PolicyNames);

                if (!response.IsTruncated)
                    break;

                await Task.Delay(100);
            }

            return results.ToArray();
        }

        public async Task<AttachedPolicyType[]> ListAttachedRolePoliciesAsync(string roleName, string patchPrefix = null, CancellationToken cancellationToken = default(CancellationToken))
        {
            ListAttachedRolePoliciesResponse response = null;
            var results = new List<AttachedPolicyType>();
            while ((response = await _IAMClient.ListAttachedRolePoliciesAsync(new ListAttachedRolePoliciesRequest()
            {
                MaxItems = 1000,
                Marker = response?.Marker,
                RoleName = roleName,
                PathPrefix = patchPrefix
            }, cancellationToken).EnsureSuccessAsync()) != null)
            {
                if (!response.AttachedPolicies.IsNullOrEmpty())
                    results.AddRange(response.AttachedPolicies);

                if (!response.IsTruncated)
                    break;

                await Task.Delay(100);
            }

            return results.ToArray();
        }

        public Task<GetRoleResponse> GetRoleAsync(string roleName, CancellationToken cancellationToken = default(CancellationToken))
            => _IAMClient.GetRoleAsync(
                new GetRoleRequest() { RoleName = roleName },
                cancellationToken).EnsureSuccessAsync();

        public Task<DetachRolePolicyResponse> DetachRolePolicyAsync(string roleName, string policyArn, CancellationToken cancellationToken = default(CancellationToken))
            => _IAMClient.DetachRolePolicyAsync(
                new DetachRolePolicyRequest() { RoleName = roleName, PolicyArn = policyArn },
                cancellationToken).EnsureSuccessAsync();

        public async Task<Role[]> ListRolesAsync(string pathPrefx = null, CancellationToken cancellationToken = default(CancellationToken))
        {
            ListRolesResponse response = null;
            var results = new List<Role>();
            while ((response = await _IAMClient.ListRolesAsync(new ListRolesRequest()
            {
                MaxItems = 1000,
                Marker = response?.Marker,
                PathPrefix = pathPrefx
            }, cancellationToken).EnsureSuccessAsync()) != null)
            {
                if (!response.Roles.IsNullOrEmpty())
                    results.AddRange(response.Roles);

                if (!response.IsTruncated)
                    break;

                await Task.Delay(100);
            }

            return results.ToArray();
        }

        public async Task<AccessKeyMetadata[]> ListAccessKeysAsync(string userName = null, CancellationToken cancellationToken = default(CancellationToken))
        {
            ListAccessKeysResponse response = null;
            var results = new List<AccessKeyMetadata>();
            while ((response = await _IAMClient.ListAccessKeysAsync(new ListAccessKeysRequest() {
                Marker = response?.Marker,
                MaxItems = 1000,
                UserName = userName
            }, cancellationToken).EnsureSuccessAsync()) != null)
            {
                if (!response.AccessKeyMetadata.IsNullOrEmpty())
                    results.AddRange(response.AccessKeyMetadata);

                if (!response.IsTruncated)
                    break;

                await Task.Delay(100);
            }

            return results.ToArray();
        }

        public async Task<PolicyVersion[]> ListPolicyVersionsAsync(string arn, CancellationToken cancellationToken = default(CancellationToken))
        {
            ListPolicyVersionsResponse response = null;
            var results = new List<PolicyVersion>();
            while ((response = await _IAMClient.ListPolicyVersionsAsync(new ListPolicyVersionsRequest()
            {
                Marker = response?.Marker,
                MaxItems = 1000,
                PolicyArn = arn
            }, cancellationToken).EnsureSuccessAsync()) != null)
            {
                if (!response.Versions.IsNullOrEmpty())
                    results.AddRange(response.Versions);

                if (!response.IsTruncated)
                    break;

                await Task.Delay(100);
            }

            return results.ToArray();
        }
    }
}
