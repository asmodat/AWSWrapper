using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AWSWrapper.Extensions;
using System.Threading;
using AsmodatStandard.Extensions;

namespace AWSWrapper.ELB
{
    public partial class ELBHelper
    {
        public async Task<IEnumerable<Amazon.ElasticLoadBalancingV2.Model.Certificate>> DescribeListenerCertificatesAsync(
            string listenerArn,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var list = new List<Amazon.ElasticLoadBalancingV2.Model.Certificate>();
            Amazon.ElasticLoadBalancingV2.Model.DescribeListenerCertificatesResponse response = null;
            while ((response = await _clientV2.DescribeListenerCertificatesAsync(
                new Amazon.ElasticLoadBalancingV2.Model.DescribeListenerCertificatesRequest()
                {
                    ListenerArn = listenerArn,
                    PageSize = 100,
                    Marker = response?.NextMarker
                }, cancellationToken))?.HttpStatusCode == System.Net.HttpStatusCode.OK)
            {
                if ((response?.Certificates?.Count ?? 0) <= 0)
                    break;
                
                list.AddRange(response.Certificates);

                if (response.NextMarker.IsNullOrEmpty())
                    break;
            }

            response.EnsureSuccess();
            return list;
        }

        public async Task<IEnumerable<Amazon.ElasticLoadBalancingV2.Model.LoadBalancer>> DescribeLoadBalancersAsync(
            IEnumerable<string> names, 
            IEnumerable<string> loadBalancerArns = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var list = new List<Amazon.ElasticLoadBalancingV2.Model.LoadBalancer>();
            Amazon.ElasticLoadBalancingV2.Model.DescribeLoadBalancersResponse response = null;
            while ((response = await _clientV2.DescribeLoadBalancersAsync(
                new Amazon.ElasticLoadBalancingV2.Model.DescribeLoadBalancersRequest()
                {
                    LoadBalancerArns = loadBalancerArns?.ToList(),
                    Names = names?.ToList(),
                    Marker = response?.NextMarker
                }, cancellationToken))?.HttpStatusCode == System.Net.HttpStatusCode.OK)
            {
                if (response?.LoadBalancers == null || response.LoadBalancers.Count <= 0)
                    break;

                list.AddRange(response.LoadBalancers);

                if (response.NextMarker.IsNullOrEmpty())
                    break;
            }

            response.EnsureSuccess();
            return list;
        }

        public async Task<IEnumerable<Amazon.ElasticLoadBalancingV2.Model.Listener>> DescribeListenersAsync(
            string loadBalancerArn,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var list = new List<Amazon.ElasticLoadBalancingV2.Model.Listener>();
            Amazon.ElasticLoadBalancingV2.Model.DescribeListenersResponse response = null;
            while ((response = await _clientV2.DescribeListenersAsync(
                new Amazon.ElasticLoadBalancingV2.Model.DescribeListenersRequest()
                {
                    LoadBalancerArn = loadBalancerArn,
                    Marker = response?.NextMarker
                }, cancellationToken))?.HttpStatusCode == System.Net.HttpStatusCode.OK)
            {
                if (response?.Listeners == null || response.Listeners.Count <= 0)
                    break;
                
                list.AddRange(response.Listeners);

                if (response.NextMarker.IsNullOrEmpty())
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
            var list = new List<Amazon.ElasticLoadBalancingV2.Model.TargetGroup>();
            Amazon.ElasticLoadBalancingV2.Model.DescribeTargetGroupsResponse response = null;
            while ((response = await _clientV2.DescribeTargetGroupsAsync(
                new Amazon.ElasticLoadBalancingV2.Model.DescribeTargetGroupsRequest()
                {
                    Names = names?.ToList(),
                    Marker = response?.NextMarker,
                    LoadBalancerArn = loadBalancerArn,
                    TargetGroupArns = targetGroupArns?.ToList()
                }, cancellationToken))?.HttpStatusCode == System.Net.HttpStatusCode.OK)
            {
                if (response?.TargetGroups == null || response.TargetGroups.Count <= 0)
                    break;

                list.AddRange(response.TargetGroups);

                if (response.NextMarker.IsNullOrEmpty())
                    break;
            }

            response.EnsureSuccess();
            return list;
        }
    }
}
