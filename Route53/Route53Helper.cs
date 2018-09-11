using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Route53;
using Amazon.Route53.Model;
using AsmodatStandard.Extensions;
using AsmodatStandard.Extensions.Collections;
using AsmodatStandard.Extensions.Threading;
using AsmodatStandard.Threading;
using AWSWrapper.Extensions;

namespace AWSWrapper.Route53
{
    public partial class Route53Helper
    {
        private readonly Stopwatch _stopWatch = Stopwatch.StartNew();
        private readonly int _rateLimit = (60*1000)/5;
        private readonly int _maxDegreeOfParalelism;
        private AmazonRoute53Client _client;
        private readonly static SemaphoreSlim _locker = new SemaphoreSlim(1, 1);

        public enum HealthCheckStatus
        {
            Unhealthy = 1,
            Healthy = 2
        }

        public Route53Helper(int maxDegreeOfParalelism = 2)
        {
            _maxDegreeOfParalelism = maxDegreeOfParalelism;
            Initialize();
        }

        private void Initialize()
        {
            _client = new AmazonRoute53Client();
        }

        public Task<ChangeTagsForResourceResponse> ChangeTagsForHealthCheckAsync(
            string id,
            IEnumerable<Tag> addTags,
            IEnumerable<string> removeTags = null,
            CancellationToken cancellationToken = default(CancellationToken))
            => _locker.Lock(() => _client.ChangeTagsForResourceAsync(new ChangeTagsForResourceRequest()
            {
                ResourceType = TagResourceType.Healthcheck,
                ResourceId = id,
                AddTags = addTags?.ToList(),
                RemoveTagKeys = removeTags?.ToList()
            }, cancellationToken).EnsureSuccessAsync());

        public async Task<ResourceTagSet[]> ListTagsForHealthChecksAsync(IEnumerable<string> resourceIds, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (resourceIds.IsNullOrEmpty())
            {
                if (resourceIds == null)
                    return null;
                else
                    return new ResourceTagSet[0];
            }
            //Batches can be max 10
            var result = await _locker.Lock(() => resourceIds.Batch(10).ForEachAsync(
                    b => _client.ListTagsForResourcesAsync(new ListTagsForResourcesRequest()
                    {
                        ResourceIds = b.ToList(),
                        ResourceType = TagResourceType.Healthcheck
                    }, cancellationToken: cancellationToken).EnsureSuccessAsync()
                , maxDegreeOfParallelism: _maxDegreeOfParalelism, cancellationToken: cancellationToken)
            );

            return result.SelectMany(x => x.ResourceTagSets).ToArray();
        }

        public Task<GetHealthCheckStatusResponse> GetHealthCheckStatusAsync(
            string id,
            CancellationToken cancellationToken = default(CancellationToken))
            => _locker.Lock(() => _client.GetHealthCheckStatusAsync(new GetHealthCheckStatusRequest()
            {
                HealthCheckId = id
            }, cancellationToken).EnsureSuccessAsync());

        public Task<UpdateHealthCheckResponse> UpdateHealthCheckAsync(
            UpdateHealthCheckRequest request,
            CancellationToken cancellationToken = default(CancellationToken))
            => _locker.Lock(() => _client.UpdateHealthCheckAsync(request, cancellationToken).EnsureSuccessAsync());

        public Task<DeleteHealthCheckResponse> DeleteHealthCheckAsync(
            string healthCheckId,
            CancellationToken cancellationToken = default(CancellationToken))
            => _locker.Lock(() => _client.DeleteHealthCheckAsync(new DeleteHealthCheckRequest()
            {
                HealthCheckId = healthCheckId
            }, cancellationToken).EnsureSuccessAsync());

        public Task<CreateHealthCheckResponse> CreateHealthCheckAsync(
            string name,
            string uri,
            int port,
            string path,
            string searchString = null,
            int failureTreshold = 1,
            CancellationToken cancellationToken = default(CancellationToken))
            => _locker.Lock(() => _client.CreateHealthCheckAsync(new CreateHealthCheckRequest()
            {
                CallerReference = name,
                HealthCheckConfig = new HealthCheckConfig()
                {
                    FullyQualifiedDomainName = uri,
                    Port = port,
                    ResourcePath = path,
                    RequestInterval = 10,
                    FailureThreshold = failureTreshold,
                    SearchString = searchString,
                    Type = searchString.IsNullOrEmpty() ? HealthCheckType.HTTP : HealthCheckType.HTTP_STR_MATCH,
                    EnableSNI = false,
                    MeasureLatency = false,
                }
            }, cancellationToken).EnsureAnyStatusCodeAsync(System.Net.HttpStatusCode.OK, System.Net.HttpStatusCode.Created));

        public Task<CreateHealthCheckResponse> CreateCloudWatchHealthCheckAsync(
            string name,
            string alarmName,
            string alarmRegion,
            bool inverted = false,
            InsufficientDataHealthStatus insufficientDataHealthStatus = null,
            CancellationToken cancellationToken = default(CancellationToken))
            => _client.CreateHealthCheckAsync(new CreateHealthCheckRequest()
            {
                CallerReference = name,
                HealthCheckConfig = new HealthCheckConfig()
                {
                    AlarmIdentifier = new AlarmIdentifier()
                    {
                        Name = alarmName,
                        Region = alarmRegion
                    },
                    Inverted = inverted,
                    InsufficientDataHealthStatus = insufficientDataHealthStatus ?? InsufficientDataHealthStatus.Unhealthy,
                    Type = HealthCheckType.CLOUDWATCH_METRIC,
                }
            }, cancellationToken).EnsureAnyStatusCodeAsync(System.Net.HttpStatusCode.OK, System.Net.HttpStatusCode.Created);

        public Task<GetHostedZoneResponse> GetHostedZoneAsync(string id, CancellationToken cancellationToken = default(CancellationToken))
            => _locker.Lock(() => _client.GetHostedZoneAsync(new GetHostedZoneRequest() { Id = id }, cancellationToken).EnsureSuccessAsync());

        public async Task<HostedZone[]> ListHostedZonesAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            ListHostedZonesResponse response = null;
            var results = new List<HostedZone>();
            while ((response = await _locker.Lock(() => _client.ListHostedZonesAsync(new ListHostedZonesRequest()
            {
                Marker = response?.NextMarker,
                
            }, cancellationToken: cancellationToken).EnsureSuccessAsync())) != null)
            {
                if (!response.HostedZones.IsNullOrEmpty())
                    results.AddRange(response.HostedZones);

                if (!response.IsTruncated)
                    break;
            }

            return results.ToArray();
        }

        public async Task<ChangeResourceRecordSetsResponse> ChangeResourceRecordSetsAsync(string zoneId, ResourceRecordSet resourceRecordSet, Change change)
        {
            var sw = Stopwatch.StartNew();
            int _timeout = 5 * 60 * 10000;
            PriorRequestNotCompleteException exception = null;
            do
            {
                try
                {
                    return await _stopWatch.TimeLock(_rateLimit, _locker, async () =>
                    {
                        return await _client.ChangeResourceRecordSetsAsync(
                               new ChangeResourceRecordSetsRequest()
                               {
                                   ChangeBatch = new ChangeBatch()
                                   {
                                       Changes = new List<Change>() { change }
                                   },
                                   HostedZoneId = zoneId
                               }).EnsureSuccessAsync();
                    });
                }
                catch (PriorRequestNotCompleteException ex) //requires client reconnection
                {
                    exception = ex;
                    await Task.Delay(5000);

                    _locker.Lock(() =>
                    {
                        Initialize();
                    });
                }
            } while (sw.ElapsedMilliseconds < _timeout);

            throw exception;
        }

        public Task DeleteResourceRecordSetsAsync(string zoneId, ResourceRecordSet resourceRecordSet)
            => ChangeResourceRecordSetsAsync(zoneId, resourceRecordSet, new Change()
            {
                Action = new ChangeAction(ChangeAction.DELETE),
                ResourceRecordSet = resourceRecordSet
            });

        public Task UpsertResourceRecordSetsAsync(string zoneId, ResourceRecordSet resourceRecordSet)
            => ChangeResourceRecordSetsAsync(zoneId, resourceRecordSet, new Change()
            {
                Action = new ChangeAction(ChangeAction.UPSERT),
                ResourceRecordSet = resourceRecordSet,
            });

        public Task UpsertResourceRecordSetsAsync(string zoneId, ResourceRecordSet oldRecordSet, ResourceRecordSet newRecordSet)
            => ChangeResourceRecordSetsAsync(zoneId, oldRecordSet, new Change()
            {
                Action = new ChangeAction(ChangeAction.UPSERT),
                ResourceRecordSet = newRecordSet,
            });
    }
}
