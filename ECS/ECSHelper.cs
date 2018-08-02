using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amazon.ECS;
using AsmodatStandard.Threading;
using AWSWrapper.Extensions;

namespace AWSWrapper.ECS
{
    public partial class ECSHelper
    {
        private readonly int _maxDegreeOfParalelism;
        private readonly AmazonECSClient _client;

        public ECSHelper(int maxDegreeOfParalelism = 8)
        {
            _maxDegreeOfParalelism = maxDegreeOfParalelism;
            _client = new AmazonECSClient();
        }

        public Task<Amazon.ECS.Model.CreateClusterResponse> CreateClusterAsync(string name, CancellationToken cancellationToken = default(CancellationToken))
            => _client.CreateClusterAsync(new Amazon.ECS.Model.CreateClusterRequest() { ClusterName = name }, cancellationToken).EnsureSuccessAsync();

        public Task DeregisterTaskDefinitionsAsync(IEnumerable<string> arns, CancellationToken cancellationToken = default(CancellationToken)) => arns.ForEachAsync(
            arn => _client.DeregisterTaskDefinitionAsync(
                    new Amazon.ECS.Model.DeregisterTaskDefinitionRequest() { TaskDefinition = arn }, cancellationToken),
                    _maxDegreeOfParalelism).EnsureSuccess();

        public Task UpdateServicesAsync(IEnumerable<string> arns, int desiredCount, string cluster, CancellationToken cancellationToken = default(CancellationToken)) => arns.ForEachAsync(
            arn => _client.UpdateServiceAsync(
                    new Amazon.ECS.Model.UpdateServiceRequest() { Service = arn, DesiredCount = desiredCount, Cluster = cluster }, cancellationToken),
                    _maxDegreeOfParalelism).EnsureSuccess();

        public Task DeleteServicesAsync(IEnumerable<string> arns, string cluster, CancellationToken cancellationToken = default(CancellationToken)) => arns.ForEachAsync(
            arn => _client.DeleteServiceAsync(new Amazon.ECS.Model.DeleteServiceRequest() { Service = arn, Cluster = cluster }, cancellationToken),
                    _maxDegreeOfParalelism).EnsureSuccess();

        public Task StopTasksAsync(IEnumerable<string> arns, string cluster, CancellationToken cancellationToken = default(CancellationToken)) 
            => arns.ForEachAsync(arn => _client.StopTaskAsync(
                    new Amazon.ECS.Model.StopTaskRequest() { Task = arn, Cluster = cluster }, cancellationToken),
                    _maxDegreeOfParalelism).EnsureSuccess();
    }
}
