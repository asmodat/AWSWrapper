using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.Route53.Model;
using AsmodatStandard.Threading;
using AsmodatStandard.Extensions.Collections;
using System.Threading;
using Amazon.Route53;
using AsmodatStandard.Extensions;

namespace AWSWrapper.Route53
{
    public static class Route53HelperEx
    {
        private static SemaphoreSlim _locker = new SemaphoreSlim(1, 1);

        public static async Task<HealthCheck> GetHealthCheckAsync(this Route53Helper r53h, string name, bool throwIfNotFound = true, CancellationToken cancellationToken = default(CancellationToken))
        {
            var healthChecks = await r53h.ListHealthChecksAsync(cancellationToken);
            var healthCheck = healthChecks.SingleOrDefault(x => x.CallerReference == name || x.Id == name);

            if (healthCheck == null && throwIfNotFound)
                throw new Exception($"Could not find any health checks with '{name}' CallerReference or Id");
            
            return healthCheck;
        }

        public static async Task DeleteHealthCheckAsync(
            this Route53Helper r53h,
            string name,
            bool thorwIfNotFound,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var hc = await r53h.GetHealthCheckAsync(name, throwIfNotFound: thorwIfNotFound, cancellationToken: cancellationToken);

            if (hc == null && !thorwIfNotFound)
                return;

            await r53h.DeleteHealthCheckAsync(hc.Id, cancellationToken);
        }

        public static async Task<HealthCheck> UpsertHealthCheckAsync(this Route53Helper r53h, 
            string name,
            string uri,
            int port,
            string path,
            int failureTreshold,
            string searchString = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var hc = r53h.GetHealthCheckAsync(name, throwIfNotFound: false, cancellationToken: cancellationToken);

            if(hc == null)
            {
                var result = await r53h.CreateHealthCheckAsync(
                    name,
                    uri,
                    port,
                    path,
                    searchString,
                    failureTreshold: failureTreshold);

                return result.HealthCheck;
            }

            var response = await r53h.UpdateHealthCheckAsync(new UpdateHealthCheckRequest()
            {
               FullyQualifiedDomainName = uri,
               Port = port,
               ResourcePath = path,
               SearchString = searchString,
               FailureThreshold = failureTreshold,
            }, cancellationToken);

            return response.HealthCheck;
        }
        
        public static async Task UpsertCNameRecordAsync(this Route53Helper r53h,
            string zoneId,
            string name,
            string value,
            int ttl = 0,
            string failover = "PRIMARY")
        {
            var zone = await r53h.GetHostedZoneAsync(zoneId);
            var cname = $"www.{name}.{zone.HostedZone.Name.TrimEnd('.')}";
            await r53h.UpsertRecordAsync(zoneId,
                 cname,
                 value,
                 RRType.CNAME,
                 ttl,
                 failover);
        }

        public static async Task<Dictionary<HostedZone, ResourceRecordSet[]>> GetRecordSets(this Route53Helper r53h, CancellationToken cancellationToken = default(CancellationToken))
        {
            var zones = await r53h.ListHostedZonesAsync(cancellationToken);
            var results = await zones.ForEachAsync(
                async zone => new KeyValuePair<HostedZone, ResourceRecordSet[]>(
                    zone, 
                    (await r53h.ListResourceRecordSetsAsync(zone.Id)).ToArray()));

            return results.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
        }

        public static async Task<ResourceRecordSet> GetCNameRecordSet(
            this Route53Helper r53h,
            string zoneId,
            string name,
            bool throwIfNotFound)
        {
            var zone = await r53h.GetHostedZoneAsync(zoneId);
            var cname = $"www.{name}.{zone.HostedZone.Name.TrimEnd('.')}";
            return await r53h.GetRecordSet(zoneId, cname, "CNAME", throwIfNotFound);
        }

            public static async Task<ResourceRecordSet> GetRecordSet(
            this Route53Helper r53h, 
            string zoneId, 
            string recordName, 
            string recordType,
            bool throwIfNotFound)
        {
            var set = await r53h.ListResourceRecordSetsAsync(zoneId);
            set = set?.Where(x => x.Name.TrimEnd('.') == recordName.TrimEnd('.') && x.Type == recordType);

            if (!throwIfNotFound && set.IsNullOrEmpty())
                return null;

            if (set?.Count() != 1)
                throw new Exception($"{nameof(GetRecordSet)} Failed, RecordSet with Name: '{recordName}' and Type: '{recordType}' was not found, or more then one was found. [{set?.Count()}]");

            return set.First();
        }

        public static async Task DestroyCNameRecord(this Route53Helper r53h, string zoneId, string name, bool throwIfNotFound = true)
        {
            var zone = await r53h.GetHostedZoneAsync(zoneId);
            var cname = $"www.{name}.{zone.HostedZone.Name.TrimEnd('.')}";
            await r53h.DestroyRecord(zoneId, cname, "CNAME", throwIfNotFound);
        }

        public static async Task DestroyRecord(this Route53Helper r53h, string zoneId, string recordName, string recordType, bool throwIfNotFound = true)
        {
            var record = await r53h.GetRecordSet(zoneId, recordName, recordType, throwIfNotFound: throwIfNotFound);

            if (!throwIfNotFound && record == null)
                return;

            await r53h.DeleteResourceRecordSetsAsync(zoneId, record);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="r53h"></param>
        /// <param name="zoneId"></param>
        /// <param name="Name"></param>
        /// <param name="Value"></param>
        /// <param name="Type"></param>
        /// <param name="TTL"></param>
        /// <param name="failover"> PRIMARY or SECONDARY</param>
        /// <param name="healthCheckId">Required if failover is set to PRIMARY</param>
        /// <returns></returns>
        public static async Task UpsertRecordAsync(this Route53Helper r53h, 
            string zoneId, string Name, string Value, RRType Type, long TTL = 0, 
            string failover = null,
            string healthCheckId = null
            )
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
                },
                Failover = failover == null ? null : new ResourceRecordSetFailover(failover),
                SetIdentifier = failover,
                HealthCheckId = healthCheckId
            });
    }
}
