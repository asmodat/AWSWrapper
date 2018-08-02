using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Route53;
using Amazon.Route53.Model;
using AsmodatStandard.Extensions;
using AWSWrapper.Extensions;

namespace AWSWrapper.Route53
{
    public partial class Route53Helper
    {
        private readonly int _maxDegreeOfParalelism;
        private readonly AmazonRoute53Client _client;

        public Route53Helper(int maxDegreeOfParalelism = 8)
        {
            _maxDegreeOfParalelism = maxDegreeOfParalelism;
            _client = new AmazonRoute53Client();
        }

        public Task<GetHostedZoneResponse> GetHostedZoneAsync(string id, CancellationToken cancellationToken = default(CancellationToken))
            => _client.GetHostedZoneAsync(new GetHostedZoneRequest() { Id = id }, cancellationToken).EnsureSuccessAsync();

        public async Task<HostedZone[]> ListHostedZonesAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            string nextToken = null;
            ListHostedZonesResponse response;
            var results = new List<HostedZone>();
            while ((response = await _client.ListHostedZonesAsync(new ListHostedZonesRequest()
            {
                Marker = null
            }, cancellationToken: cancellationToken).EnsureSuccessAsync()) != null)
            {
                if ((response.HostedZones?.Count ?? 0) == 0)
                    break;

                results.AddRange(response.HostedZones);

                if (response.NextMarker.IsNullOrEmpty())
                    break;

                nextToken = response.NextMarker;
            }

            return results.ToArray();
        }

        public Task ChangeResourceRecordSetsAsync(
            string zoneId,
            ResourceRecordSet resourceRecordSet,
            Change change) => _client.ChangeResourceRecordSetsAsync(
                     new ChangeResourceRecordSetsRequest()
                     {
                         ChangeBatch = new ChangeBatch()
                         {
                             Changes = new List<Change>() {
                                 change
                             }
                         },
                         HostedZoneId = zoneId
                     }).EnsureSuccessAsync();

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
    }
}
