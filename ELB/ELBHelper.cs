using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.ElasticLoadBalancing;
using Amazon.ElasticLoadBalancingV2;
using Amazon.ElasticLoadBalancingV2.Model;
using AsmodatStandard.Threading;
using AWSWrapper.Extensions;

namespace AWSWrapper.ELB
{
    public partial class ELBHelper
    {
        private readonly int _maxDegreeOfParalelism;
        private readonly AmazonElasticLoadBalancingClient _client;
        private readonly AmazonElasticLoadBalancingV2Client _clientV2;

        public ELBHelper(int maxDegreeOfParalelism = 8)
        {
            _maxDegreeOfParalelism = maxDegreeOfParalelism;
            _client = new AmazonElasticLoadBalancingClient();
            _clientV2 = new AmazonElasticLoadBalancingV2Client();
        }

        public Task<CreateListenerResponse> CreateListenerAsync(
            int port,
            ProtocolEnum protocol,
            string loadBalancerArn,
            string targetGroupArn,
            ActionTypeEnum actionTypeEnum,
            CancellationToken cancellationToken = default(CancellationToken))
        => _clientV2.CreateListenerAsync(
                new CreateListenerRequest()
                {
                    Port = port,
                    Protocol = protocol,
                    LoadBalancerArn = loadBalancerArn,
                    DefaultActions = new List<Action>() {
                        new Action()
                        {
                            TargetGroupArn = targetGroupArn,
                            Type = actionTypeEnum
                        }
                    }
                }
            , cancellationToken).EnsureSuccessAsync();

        public Task<CreateTargetGroupResponse> CreateTargetGroupAsync(
            string name,
            int port,
            ProtocolEnum protocol,
            string vpcId,
            TargetTypeEnum targetType,
            string healthCheckPath,
            int healthCheckIntervalSeconds,
            int healthyThresholdCount,
            int unhealthyThresholdCount,
            int healthCheckTimeoutSeconds,
            CancellationToken cancellationToken = default(CancellationToken))
        => _clientV2.CreateTargetGroupAsync(
                new CreateTargetGroupRequest()
                {
                    Name = name,
                    Port = port,
                    Protocol = protocol,
                    VpcId = vpcId,
                    TargetType = targetType,
                    HealthCheckPath = healthCheckPath,
                    HealthCheckIntervalSeconds = healthCheckIntervalSeconds,
                    HealthyThresholdCount = healthyThresholdCount,
                    UnhealthyThresholdCount = unhealthyThresholdCount,
                    HealthCheckTimeoutSeconds = healthCheckTimeoutSeconds,
                }
            , cancellationToken).EnsureSuccessAsync();

        public Task<CreateLoadBalancerResponse> CreateLoadBalancerAsync(
            string name,
            IEnumerable<string> subnets,
            IEnumerable<string> securityGroups,
            LoadBalancerTypeEnum type,
            LoadBalancerSchemeEnum scheme,
            CancellationToken cancellationToken = default(CancellationToken))
        => _clientV2.CreateLoadBalancerAsync(
                new CreateLoadBalancerRequest()
                {
                    Name = name,
                    Subnets = subnets.ToList(),
                    SecurityGroups = securityGroups.ToList(),
                    Type = type,
                    Scheme = scheme
                }
            , cancellationToken).EnsureSuccessAsync();

        public Task DeleteListenersAsync(IEnumerable<string> arns, CancellationToken cancellationToken = default(CancellationToken)) 
            => arns.ForEachAsync(arn => _clientV2.DeleteListenerAsync(
                    new DeleteListenerRequest() { ListenerArn = arn }, cancellationToken),
                    _maxDegreeOfParalelism,
                    cancellationToken
            ).EnsureSuccess();

        public Task DeleteTargetGroupsAsync(IEnumerable<string> arns, CancellationToken cancellationToken = default(CancellationToken)) 
            => arns.ForEachAsync(arn => _clientV2.DeleteTargetGroupAsync(
                    new DeleteTargetGroupRequest() { TargetGroupArn = arn }, cancellationToken),
                    _maxDegreeOfParalelism,
                    cancellationToken
            ).EnsureSuccess();

        public Task DeleteLoadBalancersAsync(IEnumerable<string> arns, CancellationToken cancellationToken = default(CancellationToken)) 
            => arns.ForEachAsync(arn => _clientV2.DeleteLoadBalancerAsync(
                    new DeleteLoadBalancerRequest() { LoadBalancerArn = arn }, cancellationToken),
                    _maxDegreeOfParalelism,
                    cancellationToken
            ).EnsureSuccess();
    }
}
