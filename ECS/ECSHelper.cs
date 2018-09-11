using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.ECS;
using Amazon.ECS.Model;
using AsmodatStandard.Threading;
using AWSWrapper.Extensions;

namespace AWSWrapper.ECS
{
    public partial class ECSHelper
    {
        private readonly int _maxDegreeOfParalelism;
        private readonly AmazonECSClient _client;
        public AmazonECSClient Client {
            get => _client;
        }

        public ECSHelper(int maxDegreeOfParalelism = 2)
        {
            _maxDegreeOfParalelism = maxDegreeOfParalelism;
            _client = new AmazonECSClient();
        }

        public Task<CreateServiceResponse> CreateServiceAsync(
            string name,
            TaskDefinition taskDefinition,
            int desiredCount,
            LaunchType launchType,
            string cluster,
            NetworkConfiguration networkConfiguration,
            LoadBalancer loadBalancer,
            CancellationToken cancellationToken = default(CancellationToken))
                => _client.CreateServiceAsync( new CreateServiceRequest() {
                    ServiceName = name,
                    TaskDefinition = $"{taskDefinition.Family}:{taskDefinition.Revision}",
                    DesiredCount = desiredCount,
                    LaunchType = launchType,
                    Cluster = cluster,
                    NetworkConfiguration = networkConfiguration,
                    LoadBalancers = new List<LoadBalancer>() { loadBalancer }
                }, cancellationToken);

        public Task<DeregisterTaskDefinitionResponse> DeregisterTaskDefinitionAsync(string taskDefinition, CancellationToken cancellationToken = default(CancellationToken))
            => _client.DeregisterTaskDefinitionAsync(new DeregisterTaskDefinitionRequest() { TaskDefinition = taskDefinition }, cancellationToken).EnsureSuccessAsync();

        public Task<RegisterTaskDefinitionResponse> RegisterTaskDefinitionAsync(
            string executionRoleArn,
            string taskRoleArn,
            string family,
            IEnumerable<string> requiresCompatibilities,
            IEnumerable<ContainerDefinition> containerDefinitions,
            int cpu,
            int memory,
            NetworkMode networkMode,
            CancellationToken cancellationToken = default(CancellationToken))
                => _client.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest()
                {
                    NetworkMode = networkMode,
                    Cpu = $"{cpu}",
                    Memory = $"{memory}",
                    ExecutionRoleArn = executionRoleArn,
                    TaskRoleArn = taskRoleArn,
                    RequiresCompatibilities = requiresCompatibilities.ToList(),
                    Family = family,
                    ContainerDefinitions = containerDefinitions.ToList()
                }, cancellationToken);

        public Task<CreateClusterResponse> CreateClusterAsync(string name, CancellationToken cancellationToken = default(CancellationToken))
            => _client.CreateClusterAsync(new CreateClusterRequest() { ClusterName = name }, cancellationToken).EnsureSuccessAsync();

        public async Task<DeleteClusterResponse> DeleteClusterAsync(string name, bool throwIfNotFound = true, CancellationToken cancellationToken = default(CancellationToken))
        {
            DeleteClusterResponse result;
            try
            {
                result = await _client.DeleteClusterAsync(new DeleteClusterRequest() { Cluster = name }, cancellationToken);
            }
            catch(ClusterNotFoundException)
            {
                if (throwIfNotFound)
                    throw;

                return null;
            }

            return result.EnsureSuccess();
        }

        public Task<DeregisterTaskDefinitionResponse[]> DeregisterTaskDefinitionsAsync(IEnumerable<string> arns, CancellationToken cancellationToken = default(CancellationToken)) => arns.ForEachAsync(
            arn => _client.DeregisterTaskDefinitionAsync(
                    new DeregisterTaskDefinitionRequest() { TaskDefinition = arn }, cancellationToken),
                    _maxDegreeOfParalelism).EnsureSuccess();

        public Task<UpdateServiceResponse[]> UpdateServicesAsync(IEnumerable<string> arns, int desiredCount, string cluster, CancellationToken cancellationToken = default(CancellationToken)) => arns.ForEachAsync(
            arn => _client.UpdateServiceAsync(
                    new UpdateServiceRequest() { Service = arn, DesiredCount = desiredCount, Cluster = cluster }, cancellationToken),
                    _maxDegreeOfParalelism).EnsureSuccess();

        public Task<DeleteServiceResponse[]> DeleteServicesAsync(IEnumerable<string> arns, string cluster, CancellationToken cancellationToken = default(CancellationToken)) => arns.ForEachAsync(
            arn => _client.DeleteServiceAsync(new DeleteServiceRequest() { Service = arn, Cluster = cluster }, cancellationToken),
                    _maxDegreeOfParalelism).EnsureSuccess();

        public Task<StopTaskResponse[]> StopTasksAsync(IEnumerable<string> arns, string cluster, CancellationToken cancellationToken = default(CancellationToken)) 
            => arns.ForEachAsync(arn => _client.StopTaskAsync(
                    new StopTaskRequest() { Task = arn, Cluster = cluster }, cancellationToken),
                    _maxDegreeOfParalelism).EnsureSuccess();
    }
}
