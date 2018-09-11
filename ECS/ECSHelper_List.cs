using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon;
using Amazon.ECS;
using AsmodatStandard.Extensions;
using AsmodatStandard.Threading;
using AsmodatStandard.Extensions.Collections;
using AWSWrapper.Extensions;

namespace AWSWrapper.ECS
{
    public partial class ECSHelper
    {
        public async Task<IEnumerable<string>> ListTaskDefinitionsAsync(string familyPrefix)
        {
            List<string> list = new List<string>();
            Amazon.ECS.Model.ListTaskDefinitionsResponse response = null;
            while ((response = await _client.ListTaskDefinitionsAsync(
                new Amazon.ECS.Model.ListTaskDefinitionsRequest()
                {
                    FamilyPrefix = familyPrefix,
                    MaxResults = 100,
                    NextToken = response?.NextToken
                }))?.HttpStatusCode == System.Net.HttpStatusCode.OK)
            {
                if (!response.TaskDefinitionArns.IsNullOrEmpty())
                    list.AddRange(response.TaskDefinitionArns);

                if (response.NextToken.IsNullOrEmpty())
                    break;

                await Task.Delay(100);
            }

            response.EnsureSuccess();
            return list;
        }

        public async Task<IEnumerable<string>> ListClustersAsync()
        {
            List<string> list = new List<string>();
            Amazon.ECS.Model.ListClustersResponse response = null;
            while ((response = await _client.ListClustersAsync(
                new Amazon.ECS.Model.ListClustersRequest()
                {
                    NextToken = response?.NextToken,
                    MaxResults = 100
                }))?.HttpStatusCode == System.Net.HttpStatusCode.OK)
            {
                if (!response.ClusterArns.IsNullOrEmpty())
                    list.AddRange(response.ClusterArns);

                if (response.NextToken.IsNullOrEmpty())
                    break;

                await Task.Delay(100);
            }

            response.EnsureSuccess();
            return list;
        }

        public async Task<IEnumerable<string>> ListServicesAsync(string cluster, LaunchType launchType)
        {
            List<string> list = new List<string>();
            Amazon.ECS.Model.ListServicesResponse response = null;
            while ((response = await _client.ListServicesAsync(
                new Amazon.ECS.Model.ListServicesRequest()
                {
                   NextToken = response?.NextToken,
                   Cluster = cluster,
                   MaxResults = 10,
                   LaunchType = launchType
                }))?.HttpStatusCode == System.Net.HttpStatusCode.OK)
            {
                if (!response.ServiceArns.IsNullOrEmpty())
                    list.AddRange(response.ServiceArns);

                if (response.NextToken.IsNullOrEmpty())
                    break;

                await Task.Delay(100);
            }

            response.EnsureSuccess();
            return list;
        }

        public async Task<IEnumerable<string>> ListTasksAsync(string cluster, string serviceName)
        {
            List<string> list = new List<string>();
            Amazon.ECS.Model.ListTasksResponse response = null;
            while ((response = await _client.ListTasksAsync(
                new Amazon.ECS.Model.ListTasksRequest()
                {
                    MaxResults = 100,
                    Cluster = cluster,
                    ServiceName = serviceName,
                    NextToken = response?.NextToken
                }))?.HttpStatusCode == System.Net.HttpStatusCode.OK)
            {
                if (!response.TaskArns.IsNullOrEmpty())
                    list.AddRange(response.TaskArns);

                if (response.NextToken.IsNullOrEmpty())
                    break;

                await Task.Delay(100);
            }

            response.EnsureSuccess();
            return list;
        }

        public async Task<IEnumerable<Amazon.ECS.Model.Service>> DescribeServicesAsync(string cluster, IEnumerable<string> services = null)
        {
            if (services != null && services.Count() > 10)
                throw new ArgumentException($"DescribeServicesAsync failed, no more then 10 services lookup allowed, but was: '{services?.Count()}'.");

            var response = await _client.DescribeServicesAsync(
                new Amazon.ECS.Model.DescribeServicesRequest()
                {
                    Cluster = cluster,
                    Services = services?.ToList()
                });
          
            response.EnsureSuccess();
            return response.Services;
        }
    }
}
