using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.ElasticLoadBalancingV2;
using System.Threading;
using Amazon.ElasticLoadBalancingV2.Model;
using AWSWrapper.Extensions;

namespace AWSWrapper.ELB
{
    public static class ELBHelperEx
    {
        public static async Task<LoadBalancer> CreateApplicationLoadBalancerAsync(
            this ELBHelper elbh,
            string name,
            IEnumerable<string> subnets,
            IEnumerable<string> securityGroups,
            bool isInternal,
            CancellationToken cancellationToken = default(CancellationToken))
            => (await elbh.CreateLoadBalancerAsync(
               name, subnets, securityGroups, LoadBalancerTypeEnum.Application,
               isInternal ? LoadBalancerSchemeEnum.Internal : LoadBalancerSchemeEnum.InternetFacing,
               cancellationToken).EnsureSuccessAsync()).LoadBalancers.Single();

        public static async Task<TargetGroup> CreateHttpTargetGroupAsync(
            this ELBHelper elbh,
            string name,
            int port,
            string vpcId,
            string healthCheckPath,
            CancellationToken cancellationToken = default(CancellationToken))
            => (await elbh.CreateTargetGroupAsync(
                name,
                port,
                ProtocolEnum.HTTP,
                vpcId,
                TargetTypeEnum.Ip,
                healthCheckPath,
                healthCheckIntervalSeconds: 30,
                healthyThresholdCount: 3,
                unhealthyThresholdCount: 2,
                healthCheckTimeoutSeconds: 5,
                cancellationToken: cancellationToken).EnsureSuccessAsync()).TargetGroups.Single();

        public static async Task<Listener> CreateHttpListenerAsync(
            this ELBHelper elbh,
            string loadBalancerArn,
            string targetGroupArn,
            int port,
            CancellationToken cancellationToken = default(CancellationToken))
            => (await elbh.CreateListenerAsync(
                port,
                ProtocolEnum.HTTP,
                loadBalancerArn,
                targetGroupArn,
                ActionTypeEnum.Forward,
                cancellationToken).EnsureSuccessAsync()).Listeners.Single();

        public static async Task<IEnumerable<string>> ListListenersAsync(this ELBHelper elbh, string loadBalancerArn, CancellationToken cancellationToken = default(CancellationToken))
           => (await elbh.DescribeListenersAsync(loadBalancerArn)).Select(x => x.ListenerArn);

        public static async Task<IEnumerable<string>> ListTargetGroupsAsync(
            this ELBHelper elbh,
            string loadBalancerArn,
            IEnumerable<string> names = null, 
            IEnumerable<string> targetGroupArns = null, 
            CancellationToken cancellationToken = default(CancellationToken))
            => (await elbh.DescribeTargetGroupsAsync(loadBalancerArn, names, targetGroupArns, cancellationToken)).Select(x => x.TargetGroupArn);

        public static async Task DestroyLoadBalancer(this ELBHelper elbh, string loadBalancerName, bool throwIfNotFound, CancellationToken cancellationToken = default(CancellationToken))
        {
            IEnumerable<LoadBalancer> loadbalancers;

            if (!throwIfNotFound)
            {
                try
                {
                    loadbalancers = await elbh.DescribeLoadBalancersAsync(new List<string>() { loadBalancerName });
                }
                catch(LoadBalancerNotFoundException ex)
                {
                    return;
                }
            }
            else
                loadbalancers = await elbh.DescribeLoadBalancersAsync(new List<string>() { loadBalancerName });

            if (loadbalancers.Count() != 1)
            {
                if (throwIfNotFound)
                    throw new Exception($"DestroyLoadBalancer, LoadBalancer '{loadBalancerName}' was not found, or multiple load balancers with the same name were found.");
                else
                    return;
            }

            var arn = loadbalancers.First().LoadBalancerArn;
            var listeners = await elbh.ListListenersAsync(arn, cancellationToken);
            var targetGroups = await elbh.ListTargetGroupsAsync(arn, cancellationToken: cancellationToken);

            //kill listeners
            await elbh.DeleteListenersAsync(listeners, cancellationToken);

            //kill target groups
            await elbh.DeleteTargetGroupsAsync(targetGroups, cancellationToken);

            //kill loadbalancer
            await elbh.DeleteLoadBalancersAsync(new List<string>() { arn }, cancellationToken);
        }
    }
}
