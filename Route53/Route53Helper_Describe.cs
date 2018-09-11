using System.Collections.Generic;
using System.Threading.Tasks;
using AWSWrapper.Extensions;
using System.Threading;
using AsmodatStandard.Extensions.Collections;

namespace AWSWrapper.Route53
{
    public partial class Route53Helper
    {
        public async Task<IEnumerable<Amazon.Route53.Model.HealthCheck>> ListHealthChecksAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            var list = new List<Amazon.Route53.Model.HealthCheck>();
            Amazon.Route53.Model.ListHealthChecksResponse response = null;
            while ((response = await _client.ListHealthChecksAsync(
                new Amazon.Route53.Model.ListHealthChecksRequest()
                {
                    Marker = response?.NextMarker,
                    MaxItems = "100"
                }, cancellationToken))?.HttpStatusCode == System.Net.HttpStatusCode.OK)
            {
                if (!response.HealthChecks.IsNullOrEmpty())
                    list.AddRange(response.HealthChecks);

                if (!response.IsTruncated)
                    break;

                await Task.Delay(100);
            }
            
            response.EnsureSuccess();
            return list;
        }

        public async Task<IEnumerable<Amazon.Route53.Model.ResourceRecordSet>> ListResourceRecordSetsAsync(string zoneId, CancellationToken cancellationToken = default(CancellationToken))
        {
            var list = new List<Amazon.Route53.Model.ResourceRecordSet>();
            Amazon.Route53.Model.ListResourceRecordSetsResponse response = null;
            while ((response = await _client.ListResourceRecordSetsAsync(
                new Amazon.Route53.Model.ListResourceRecordSetsRequest()
                {
                    StartRecordIdentifier = response?.NextRecordIdentifier,
                    StartRecordName = response?.NextRecordName,
                    StartRecordType = response?.NextRecordType,
                    HostedZoneId = zoneId,
                    MaxItems = "1000",
                }, cancellationToken))?.HttpStatusCode == System.Net.HttpStatusCode.OK)
            {
                if (!response.ResourceRecordSets.IsNullOrEmpty())
                    list.AddRange(response.ResourceRecordSets);

                if (!response.IsTruncated)
                    break;

                await Task.Delay(100);
            }

            response.EnsureSuccess();
            return list;
        }

    }
}
