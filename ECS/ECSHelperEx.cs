using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon;
using Amazon.ECS;
using AsmodatStandard.Extensions;
using AsmodatStandard.Threading;
using AsmodatStandard.Extensions.Collections;
using AsmodatStandard.Types;
using Amazon.Route53;
using AWSWrapper.Extensions;
using Amazon.ECS.Model;
using System.Threading;
using System.Diagnostics;

namespace AWSWrapper.ECS
{
    public static class ECSHelperEx
    {
        public static async Task<ServiceInfo> GetServiceOrDefault(this ECSHelper ecs, string cluster, string serviceName)
        {
            var services = await ((cluster.IsNullOrEmpty()) ? ecs.ListServicesAsync() : ecs.ListServicesAsync(cluster));
            return services.SingleOrDefault(
                x => ((serviceName.StartsWith("arn:")) ? x.ARN == serviceName : x.ARN.EndsWith($":service/{serviceName}")));
        }

        public static async Task<IEnumerable<ServiceInfo>> ListServicesAsync(this ECSHelper ecs)
        {
            var clusetrs = await ecs.ListClustersAsync();
            var result = await clusetrs.ForEachAsync(cluster => ListServicesAsync(ecs, cluster), 8);
            return result.Flatten();
        }

        public static async Task<Service> CreateFargateServiceAsync(this ECSHelper ecs,
            string name,
            TaskDefinition taskDefinition,
            int desiredCount,
            string cluster,
            Amazon.ElasticLoadBalancingV2.Model.TargetGroup targetGroup,
            bool assignPublicIP,
            IEnumerable<string> securityGroups,
            IEnumerable<string> subnets,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var nc = new NetworkConfiguration() {
                AwsvpcConfiguration = new AwsVpcConfiguration() {
                    AssignPublicIp = assignPublicIP ? AssignPublicIp.ENABLED : AssignPublicIp.DISABLED,
                    SecurityGroups = securityGroups.ToList(),
                    Subnets = subnets.ToList()
                }
            };

            var result = await ecs.CreateServiceAsync(name: name,
                taskDefinition: taskDefinition,
                desiredCount: desiredCount,
                launchType: LaunchType.FARGATE,
                cluster: cluster,
                networkConfiguration: nc,
                loadBalancer: new LoadBalancer() {
                    ContainerPort = targetGroup.Port,
                    TargetGroupArn = targetGroup.TargetGroupArn,
                    ContainerName = taskDefinition.ContainerDefinitions.Single().Name,
                },
                cancellationToken: cancellationToken);

            return result.Service;
        }

        public static async Task<TaskDefinition> RegisterFargateTaskAsync(this ECSHelper ecs,
            string executionRoleArn,
            string family,
            int cpu,
            int memory,
            string name,
            string image,
            Dictionary<string, string> envVariables,
            string logGroup,
            IEnumerable<int> ports,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var env = envVariables?.Select(x => new KeyValuePair() { Name = x.Key, Value = x.Value })?.ToList();
            var lc = new LogConfiguration()
            {
                LogDriver = new LogDriver("awslogs"),
                Options = new Dictionary<string, string>() {
                    { "awslogs-group", logGroup },
                    { "awslogs-region", ecs.Client.GetRegionName() },
                    { "awslogs-stream-prefix", "ecs" }
                }
            };

            var pmTCP = ports.Select(x => new PortMapping() { ContainerPort = x, HostPort = x, Protocol = TransportProtocol.Tcp });

            var cd = new ContainerDefinition()
            {
                Name = name,
                Image = image,
                Environment = env,
                LogConfiguration = lc,
                PortMappings = pmTCP.ToList(),
                MemoryReservation = (int)(memory * 4) / 5
            };

            var response = await ecs.RegisterTaskDefinitionAsync(
                executionRoleArn,
                executionRoleArn,
                family,
                new string[] { "FARGATE" },
                new List<ContainerDefinition>() { cd },
                cpu,
                memory,
                NetworkMode.Awsvpc,
                cancellationToken);

            return response.TaskDefinition;
        }

        public static async Task<IEnumerable<ServiceInfo>> ListServicesAsync(this ECSHelper ecs, string cluster, bool throwIfNotFound = true)
        {
            IEnumerable<string> fargateServices = null;
            IEnumerable<string> ec2Services = null;
            try
            {
                fargateServices = await ecs.ListServicesAsync(cluster, LaunchType.FARGATE);
                ec2Services = await ecs.ListServicesAsync(cluster, LaunchType.EC2);
            }
            catch(ClusterNotFoundException)
            {
                if (throwIfNotFound)
                    throw;

                return null;
            }

            return fargateServices.Select(x => new ServiceInfo(cluster: cluster, arn: x, launchType: LaunchType.FARGATE))
                .ConcatOrDefault(ec2Services.Select(x => new ServiceInfo(cluster: cluster, arn: x, launchType: LaunchType.EC2)));
        }

        public static System.Threading.Tasks.Task UpdateServiceAsync(this ECSHelper ecs, int desiredCount, string cluster, params string[] arns)
            => ecs.UpdateServicesAsync(arns, desiredCount, cluster);

        public static Task<DeleteServiceResponse[]> DeleteServiceAsync(this ECSHelper ecs, string cluster, params string[] arns)
            => ecs.DeleteServicesAsync(arns, cluster: cluster);

        public static async System.Threading.Tasks.Task DestroyService(this ECSHelper ecs, string cluster, string serviceName, bool throwIfNotFound = true, int drainingTimeout = 5*60*1000)
        {
            var services = await ((cluster.IsNullOrEmpty()) ? ecs.ListServicesAsync() : ecs.ListServicesAsync(cluster, throwIfNotFound: throwIfNotFound));

            services = services?.Where(x => ((serviceName.StartsWith("arn:")) ? x.ARN == serviceName : x.ARN.EndsWith($":service/{serviceName}")));

            if (!throwIfNotFound && (services?.Count() ?? 0) == 0)
                return;

            if (services?.Count() != 1)
                throw new Exception($"Could not find service '{serviceName}' for cluster: '{cluster}' or found more then one matching result (In such case use ARN insted of serviceName, or specify cluster) [{services?.Count()}].");

            var service = services.First();

            var tasks = await ecs.ListTasksAsync(service.Cluster, service.ARN);
            if ((tasks?.Count() ?? 0) != 0)
                await ecs.StopTasksAsync(arns: tasks, cluster: service.Cluster);

            await ecs.UpdateServiceAsync(desiredCount: 0, arns: service.ARN, cluster: service.Cluster);
            await ecs.DeleteServiceAsync(cluster: service.Cluster, arns: service.ARN);

            //ensure service is in draining state
            await System.Threading.Tasks.Task.Delay(1000);

            string status = null;
            var sw = Stopwatch.StartNew();

            //ensure service is not in draining state before finishing
            while ((status = ((await ecs.DescribeServicesAsync(cluster: cluster, services: new string[] { serviceName })).FirstOrDefault())?.Status) == "DRAINING")
            {
                if (sw.ElapsedMilliseconds > drainingTimeout)
                    throw new Exception($"Could not drain the service '{serviceName}' for cluster: '{cluster}', elapsed {sw.ElapsedMilliseconds}/{drainingTimeout} [ms].");

                await System.Threading.Tasks.Task.Delay(1000);
            }
        }

        public static async System.Threading.Tasks.Task DestroyTaskDefinitions(this ECSHelper ecs, string familyPrefix)
        {
            var tasks = await ecs.ListTaskDefinitionsAsync(familyPrefix);

            if(!tasks.IsNullOrEmpty())
                await ecs.DeregisterTaskDefinitionsAsync(tasks);
        }

        public static async System.Threading.Tasks.Task WaitForServiceToStart(this ECSHelper ecs, string cluster, string serviceName, int timeout)
        {
            var services = await ((cluster.IsNullOrEmpty()) ? ecs.ListServicesAsync() : ecs.ListServicesAsync(cluster));

            services = services.Where(x => ((serviceName.StartsWith("arn:")) ? x.ARN == serviceName : x.ARN.EndsWith($":service/{serviceName}")));

            if (services?.Count() != 1)
                throw new Exception($"Could not find service '{serviceName}' for cluster: '{cluster}' or found more then one matching result (In such case use ARN insted of serviceName, or specify cluster) [{services?.Count()}].");

            var service = services.First();

            var tt = new TickTimeout(timeout, TickTime.Unit.s, Enabled: true);
            while (!tt.IsTriggered)
            {
                var serviceDescription = await ecs.DescribeServicesAsync(service.Cluster, new string[] { service.ARN });

                if (serviceDescription.IsNullOrEmpty())
                    throw new Exception($"Could not find or describe service: '{service.ARN}' for the cluster '{service.Cluster}'.");

                var result = serviceDescription.First();

                if (result.DesiredCount == result.RunningCount)
                    return; //desired count reached

                if (result.PendingCount != 0)
                    await System.Threading.Tasks.Task.Delay(1000);
            }

            throw new Exception($"Timeout '{timeout}' [s], service: '{service.ARN}' could not reach its desired count in time.");
        }
    }
}
