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

        public static async Task<TargetGroup> CreateHttpsTargetGroupAsync(
            this ELBHelper elbh,
            string name,
            int port,
            string vpcId,
            string healthCheckPath,
            CancellationToken cancellationToken = default(CancellationToken))
            => (await elbh.CreateTargetGroupAsync(
                name,
                port,
                ProtocolEnum.HTTPS,
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
                null,
                null,
                cancellationToken).EnsureSuccessAsync()).Listeners.Single();

        public static async Task<Listener> CreateHttpsListenerAsync(
            this ELBHelper elbh,
            string loadBalancerArn,
            string targetGroupArn,
            int port,
            CancellationToken cancellationToken = default(CancellationToken))
        {



            var l = (await elbh.CreateListenerAsync(
                port,
                ProtocolEnum.HTTPS,
                loadBalancerArn,
                targetGroupArn,
                ActionTypeEnum.Forward,
                null,
                "ELBSecurityPolicy-2016-08",
                cancellationToken).EnsureSuccessAsync()).Listeners.Single();

            return l;
        }

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
            var loadbalancers = await elbh.GetLoadBalancersByName(loadBalancerName, throwIfNotFound, cancellationToken);

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

        public static async Task<TargetGroup> GetTargetGroupByName(this ELBHelper elbh, string name, LoadBalancer lb, bool throwIfNotFound, CancellationToken cancellationToken = default(CancellationToken))
        {
            var tgs = await elbh.DescribeTargetGroupsAsync(lb.LoadBalancerArn);
            var tg = tgs.SingleOrDefault(x => x.TargetGroupName == name);

            if (throwIfNotFound && tg == null)
                throw new Exception($"Could not find Target Group '{name}' for Load Balancer '{lb.LoadBalancerName}'");

            return tg;
        }

        public static async Task<IEnumerable<LoadBalancer>> GetLoadBalancersByName(this ELBHelper elbh, string loadBalancerName, bool throwIfNotFound, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (!throwIfNotFound)
            {
                try
                {
                    return await elbh.DescribeLoadBalancersAsync(new List<string>() { loadBalancerName });
                }
                catch (LoadBalancerNotFoundException ex)
                {
                    return new LoadBalancer[0];
                }
            }
            else
                return await elbh.DescribeLoadBalancersAsync(new List<string>() { loadBalancerName });
        }

        public static async Task<IEnumerable<TargetGroup>> DescribeLoadBalancerTargetGroupsAsync(
            this ELBHelper elbh,
            string loadBalancerName,
            bool thowIfNotFound,
             CancellationToken cancellationToken = default(CancellationToken))
        {
            var loadbalancer = (await elbh.GetLoadBalancersByName(loadBalancerName, thowIfNotFound, cancellationToken)).SingleOrDefault();

            if (!thowIfNotFound)
                return null;

            return await elbh.DescribeTargetGroupsAsync(loadBalancerArn: loadbalancer.LoadBalancerArn, cancellationToken: cancellationToken);
        }
    }
}
