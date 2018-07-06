﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.IdentityManagement;
using Amazon.IdentityManagement.Model;
using AWSWrapper.Extensions;
using AsmodatStandard.Extensions;
using AsmodatStandard.Extensions.Collections;

namespace AWSWrapper.IAM
{
    public partial class IAMHelper
    {
        internal readonly int _maxDegreeOfParalelism;
        internal readonly AmazonIdentityManagementServiceClient _IAMClient;

        public IAMHelper(int maxDegreeOfParalelism = 8)
        {
            _maxDegreeOfParalelism = maxDegreeOfParalelism;
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
            string path = null, 
            int maxSessionDuration = 12,
            string assumeRolePolicyDocument = null,
            CancellationToken cancellationToken = default(CancellationToken))
            => _IAMClient.CreateRoleAsync(
                new CreateRoleRequest() { Description = description, RoleName = roleName, MaxSessionDuration = maxSessionDuration, AssumeRolePolicyDocument = assumeRolePolicyDocument, Path = path },
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
                new DeletePolicyRequest() { PolicyArn = arn  },
                cancellationToken).EnsureSuccessAsync();

        public async Task<ManagedPolicy[]> ListPoliciesAsync(string pathPrefx = null, bool onlyAttached = false, CancellationToken cancellationToken = default(CancellationToken))
        {
            string nextToken = null;
            ListPoliciesResponse response;
            var results = new List<ManagedPolicy>();
            while ((response = await _IAMClient.ListPoliciesAsync(new ListPoliciesRequest()
            {
                MaxItems = 100,
                Scope = PolicyScopeType.All,
                OnlyAttached = onlyAttached,
                Marker = nextToken,
                PathPrefix = pathPrefx
            }, cancellationToken).EnsureSuccessAsync()) != null)
            {
                if ((response.Policies?.Count ?? 0) == 0)
                    break;

                results.AddRange(response.Policies);

                if (response.Marker.IsNullOrEmpty())
                    break;

                nextToken = response.Marker;
            }

            return results.ToArray();
        }

        public async Task<Role[]> ListRolesAsync(string pathPrefx = null, CancellationToken cancellationToken = default(CancellationToken))
        {
            string nextToken = null;
            ListRolesResponse response;
            var results = new List<Role>();
            while ((response = await _IAMClient.ListRolesAsync(new ListRolesRequest()
            {
                MaxItems = 100,
                Marker = nextToken,
                PathPrefix = pathPrefx
            }, cancellationToken).EnsureSuccessAsync()) != null)
            {
                if ((response.Roles?.Count ?? 0) == 0)
                    break;

                results.AddRange(response.Roles);

                if (response.Marker.IsNullOrEmpty())
                    break;

                nextToken = response.Marker;
            }

            return results.ToArray();
        }
    }
}