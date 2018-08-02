using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon;
using Amazon.ECS;
using AsmodatStandard.Extensions;
using AsmodatStandard.Threading;
using AsmodatStandard.Extensions.Collections;
using Amazon.ElasticLoadBalancingV2;
using System.Threading;
using Amazon.ElasticLoadBalancingV2.Model;

namespace AWSWrapper.ELB
{
    public static class ELBHelperEx
    {
        public static async Task<(
            TargetGroup targetGroup, 
            LoadBalancer loadBalancer,
            Listener listener
            )> CreateHttpApplicationLoadBalancer(
            this ELBHelper elbh,
            string name,
            IEnumerable<string> subnets,
            IEnumerable<string> securityGroups,
            bool isInternal,
            int port,
            string vpcId,
            string healthCheckPath,
            CancellationToken cancellationToken = default(CancellationToken) )
        {

            var albName = $"{name}-alb-{(isInternal ? "prv" : "pub")}";
            var tgName = $"{name}-tg-{(isInternal ? "prv" : "pub")}";

            await elbh.DestroyLoadBalancer(loadBalancerName: albName, throwIfNotFound: false, cancellationToken: cancellationToken);

            var tAlb = elbh.CreateLoadBalancerAsync(albName, subnets, securityGroups, LoadBalancerTypeEnum.Application,
               isInternal ? LoadBalancerSchemeEnum.Internal : LoadBalancerSchemeEnum.InternetFacing, cancellationToken);

            var tTg = elbh.CreateTargetGroupAsync(
                tgName,
                port,
                ProtocolEnum.HTTP,
                vpcId,
                TargetTypeEnum.Ip,
                healthCheckPath,
                healthCheckIntervalSeconds: 30,
                healthyThresholdCount: 3,
                unhealthyThresholdCount: 2,
                healthCheckTimeoutSeconds: 5,
                cancellationToken: cancellationToken);

            var alb = (await tAlb).LoadBalancers.Single();
            var tg = (await tTg).TargetGroups.Single();

            var tL = elbh.CreateListenerAsync(
                port,
                ProtocolEnum.HTTP,
                alb.LoadBalancerArn,
                tg.TargetGroupArn,
                ActionTypeEnum.Forward,
                cancellationToken);

            var l = (await tL).Listeners.Single();

            return (tg, alb, l);
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
