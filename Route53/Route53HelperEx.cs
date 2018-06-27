using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.Route53.Model;
using AsmodatStandard.Threading;
using AsmodatStandard.Extensions.Collections;
using System.Threading;
using Amazon.Route53;

namespace AWSWrapper.Route53
{
    public static class Route53HelperEx
    {
        private static SemaphoreSlim _locker = new SemaphoreSlim(1, 1);

        public static async Task<Dictionary<HostedZone, ResourceRecordSet[]>> GetRecordSets(this Route53Helper r53h, CancellationToken cancellationToken = default(CancellationToken))
        {
            var zones = await r53h.ListHostedZonesAsync(cancellationToken);
            var results = await zones.ForEachAsync(
                async zone => new KeyValuePair<HostedZone, ResourceRecordSet[]>(
                    zone, 
                    (await r53h.ListResourceRecordSetsAsync(zone.Id)).ToArray()));

            return results.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
        }

        public static async Task<ResourceRecordSet> GetRecordSet(this Route53Helper r53h, string zoneId, string recordName, string recordType)
        {
            var set = await r53h.ListResourceRecordSetsAsync(zoneId);
            set = set?.Where(x => x.Name == recordName && x.Type == recordType);

            if (set?.Count() != 1)
                throw new Exception($"{nameof(GetRecordSet)} Failed, RecordSet with Name: '{recordName}' and Type: '{recordType}' was not found, or more then one was found. [{set?.Count()}]");

            return set.First();
        }

        public static async Task DestroyRecord(this Route53Helper r53h, string zoneId, string recordName, string recordType)
            => await r53h.DeleteResourceRecordSetsAsync(zoneId, await r53h.GetRecordSet(zoneId, recordName, recordType));

        public static async Task UpsertRecord(this Route53Helper r53h, string zoneId, string Name, string Value, RRType Type, long TTL = 60)
            => await r53h.UpsertResourceRecordSetsAsync(zoneId, new ResourceRecordSet() {
                Name = Name,
                TTL = TTL,
                Type = Type,
                ResourceRecords = new List<ResourceRecord>()
                {
                    new ResourceRecord()
                    {
                        Value = Value
                    }
                }
            });
    }
}
