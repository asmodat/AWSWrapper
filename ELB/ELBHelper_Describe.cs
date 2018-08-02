﻿using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AWSWrapper.Extensions;
using System.Threading;

namespace AWSWrapper.ELB
{
    public partial class ELBHelper
    {
        public async Task<IEnumerable<Amazon.ElasticLoadBalancingV2.Model.LoadBalancer>> DescribeLoadBalancersAsync(
            IEnumerable<string> names, 
            IEnumerable<string> loadBalancerArns = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            string token = null;
            var list = new List<Amazon.ElasticLoadBalancingV2.Model.LoadBalancer>();
            Amazon.ElasticLoadBalancingV2.Model.DescribeLoadBalancersResponse response;
            while ((response = await _clientV2.DescribeLoadBalancersAsync(
                new Amazon.ElasticLoadBalancingV2.Model.DescribeLoadBalancersRequest()
                {
                    LoadBalancerArns = loadBalancerArns?.ToList(),
                    Names = names?.ToList(),
                    Marker = token
                }, cancellationToken))?.HttpStatusCode == System.Net.HttpStatusCode.OK)
            {
                if (response?.LoadBalancers == null || response.LoadBalancers.Count <= 0)
                    break;

                list.AddRange(response.LoadBalancers);

                token = response.NextMarker;
                if (token == null)
                    break;
            }

            response.EnsureSuccess();
            return list;
        }

        public async Task<IEnumerable<Amazon.ElasticLoadBalancingV2.Model.Listener>> DescribeListenersAsync(
            string loadBalancerArn,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            string token = null;
            var list = new List<Amazon.ElasticLoadBalancingV2.Model.Listener>();
            Amazon.ElasticLoadBalancingV2.Model.DescribeListenersResponse response;
            while ((response = await _clientV2.DescribeListenersAsync(
                new Amazon.ElasticLoadBalancingV2.Model.DescribeListenersRequest()
                {
                    LoadBalancerArn = loadBalancerArn,
                    Marker = token
                }, cancellationToken))?.HttpStatusCode == System.Net.HttpStatusCode.OK)
            {
                if (response?.Listeners == null || response.Listeners.Count <= 0)
                    break;

                list.AddRange(response.Listeners);

                token = response.NextMarker;
                if (token == null)
                    break;
            }

            response.EnsureSuccess();
            return list;
        }

        public async Task<IEnumerable<Amazon.ElasticLoadBalancingV2.Model.TargetGroup>> DescribeTargetGroupsAsync(
            string loadBalancerArn, 
            IEnumerable<string> names = null,
             IEnumerable<string> targetGroupArns = null, 
             CancellationToken cancellationToken = default(CancellationToken))
        {
            string token = null;
            var list = new List<Amazon.ElasticLoadBalancingV2.Model.TargetGroup>();
            Amazon.ElasticLoadBalancingV2.Model.DescribeTargetGroupsResponse response;
            while ((response = await _clientV2.DescribeTargetGroupsAsync(
                new Amazon.ElasticLoadBalancingV2.Model.DescribeTargetGroupsRequest()
                {
                    Names = names?.ToList(),
                    Marker = token,
                    LoadBalancerArn = loadBalancerArn,
                    TargetGroupArns = targetGroupArns?.ToList()
                }, cancellationToken))?.HttpStatusCode == System.Net.HttpStatusCode.OK)
            {
                if (response?.TargetGroups == null || response.TargetGroups.Count <= 0)
                    break;

                list.AddRange(response.TargetGroups);

                token = response.NextMarker;
                if (token == null)
                    break;
            }

            response.EnsureSuccess();
            return list;
        }
    }
}
