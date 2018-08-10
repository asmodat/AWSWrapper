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
using System.Diagnostics;
using AsmodatStandard.Extensions.Threading;
using static AWSWrapper.Route53.Route53Helper;

namespace AWSWrapper.Route53
{
    public static class Route53HelperEx
    {
        private static SemaphoreSlim _locker = new SemaphoreSlim(1, 1);


        public static async Task<StatusReport> WaitForHealthCheckAsync(this Route53Helper r53h, string name,
            HealthCheckStatus status, //Unhealthy, Healthy
            int timeout_s)
        {
            var sw = Stopwatch.StartNew();
            var hc = await r53h.GetHealthCheckAsync(name, throwIfNotFound: true);
            StatusReport report;
            var healthyStatus = hc.HealthCheckConfig.Inverted ? HealthCheckStatus.Unhealthy : HealthCheckStatus.Healthy;
            var unHealthyStatus = hc.HealthCheckConfig.Inverted ? HealthCheckStatus.Healthy : HealthCheckStatus.Unhealthy;
            do
            {
                var hcs = await r53h.GetHealthCheckStatusAsync(hc.Id);
                report = hcs.HealthCheckObservations.OrderByDescending(x => x.StatusReport.CheckedTime).First().StatusReport;

                if ((report.Status.ContainsAny("Success", HealthCheckStatus.Healthy.ToString()) && status == healthyStatus) ||
                    (report.Status.ContainsAny("Failure", HealthCheckStatus.Unhealthy.ToString()) && status == unHealthyStatus))
                    return report;

                await _locker.Lock(() => Task.Delay(1000));
            }
            while (sw.ElapsedMilliseconds < (timeout_s * 1000));

            throw new Exception($"Health Check '{name}' coudn't reach '{status}' status within {timeout_s} [s], last state was: '{report.JsonSerialize(Newtonsoft.Json.Formatting.Indented)}'.");
        }

        public static async Task<bool> HealthCheckExistsAsync(this Route53Helper r53h, string name, bool throwIfNotFound = true, CancellationToken cancellationToken = default(CancellationToken))
           => (await r53h.GetHealthCheckAsync(name, throwIfNotFound: false, cancellationToken: cancellationToken)) != null;

        public static async Task<HealthCheck> GetHealthCheckAsync(this Route53Helper r53h, string name, bool throwIfNotFound = true, CancellationToken cancellationToken = default(CancellationToken))
        {
            var healthChecks = await r53h.ListHealthChecksAsync(cancellationToken);
            var healthCheck = healthChecks.SingleOrDefault(x => x.CallerReference?.StartsWith(name) == true || x.Id == name);

            if(healthCheck == null && !healthChecks.IsNullOrEmpty())
            {
                var tagSets = await r53h.ListTagsForHealthChecksAsync(healthChecks.Select(x => x.Id), cancellationToken);
                var set = tagSets.SingleOrDefault(x => x.Tags.Any(y => y.Key == "Name" && y.Value?.StartsWith(name) == true));

                if(set != null)
                    healthCheck = healthChecks.SingleOrDefault(x => x.Id == set.ResourceId);
            }

            if (healthCheck == null && throwIfNotFound)
                throw new Exception($"Could not find any health checks with '{name}' CallerReference or Id");

            return healthCheck;
        }

        public static async Task<DeleteHealthCheckResponse> DeleteHealthCheckByNameAsync(
            this Route53Helper r53h,
            string name,
            bool throwIfNotFound,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var hc = await r53h.GetHealthCheckAsync(name, throwIfNotFound: throwIfNotFound, cancellationToken: cancellationToken);

            if (hc == null && !throwIfNotFound)
                return new DeleteHealthCheckResponse() { HttpStatusCode = System.Net.HttpStatusCode.NotFound };

            return await r53h.DeleteHealthCheckAsync(hc.Id, cancellationToken);
        }
        
        public static async Task<HealthCheck> UpsertCloudWatchHealthCheckAsync(this Route53Helper r53h,
            string name,
            string alarmName,
            string alarmRegion,
            bool inverted = false,
            InsufficientDataHealthStatus insufficientDataHealthStatus = null,
            bool throwIfNotFound = true,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var hc = await r53h.GetHealthCheckAsync(name, throwIfNotFound: throwIfNotFound, cancellationToken: cancellationToken);

            if (hc == null)
            {
                hc = (await r53h.CreateCloudWatchHealthCheckAsync(
                    $"{name}-{Guid.NewGuid().ToString()}",
                    alarmName,
                    alarmRegion,
                    inverted,
                    insufficientDataHealthStatus,
                    cancellationToken)).HealthCheck;

                await r53h.ChangeTagsForHealthCheckAsync(hc.Id, new Tag[] { new Tag() { Key = "Name", Value = name } });

                await Task.Delay(1000); //ensure record was created

                return hc;
            }

            var response = await r53h.UpdateHealthCheckAsync(new UpdateHealthCheckRequest()
            {
                HealthCheckId = hc.Id,
                AlarmIdentifier = new AlarmIdentifier()
                {
                    Name = alarmName,
                    Region = alarmRegion,
                },
                Inverted = inverted,
                InsufficientDataHealthStatus = insufficientDataHealthStatus ?? InsufficientDataHealthStatus.Unhealthy
            }, cancellationToken);

            await Task.Delay(1000); //ensure record was updated

            return response.HealthCheck;
        }

        public static async Task<HealthCheck> UpsertHealthCheckAsync(this Route53Helper r53h,
            string name,
            string uri,
            int port,
            string path,
            int failureTreshold,
            string searchString = null,
            bool throwIfNotFound = true,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var hc = await r53h.GetHealthCheckAsync(name, throwIfNotFound: throwIfNotFound, cancellationToken: cancellationToken);

            if (hc == null)
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
                HealthCheckId = hc.Id,
                FullyQualifiedDomainName = uri,
                Port = port,
                ResourcePath = path,
                SearchString = searchString,
                FailureThreshold = failureTreshold,
                EnableSNI = hc.HealthCheckConfig.EnableSNI,
            }, cancellationToken);

            return response.HealthCheck;
        }

        public static async Task<string> UpsertCNameRecordAsync(this Route53Helper r53h,
            string zoneId,
            string name,
            string value,
            int ttl = 0,
            string failover = null,
            string healthCheckId = null,
            string setIdentifier = null)
        {
            var zone = await r53h.GetHostedZoneAsync(zoneId);
            var cname = $"www.{name}.{zone.HostedZone.Name.TrimEnd('.')}";
            await r53h.UpsertRecordAsync(zoneId,
                 cname,
                 value,
                 RRType.CNAME,
                 ttl,
                 failover,
                 healthCheckId,
                 setIdentifier);
            return cname;
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
            string failover = null,
            bool throwIfNotFound = true)
        {
            var zone = await r53h.GetHostedZoneAsync(zoneId);
            var cname = $"www.{name}.{zone.HostedZone.Name.TrimEnd('.')}";
            return await r53h.GetRecordSet(zoneId, cname, "CNAME", failover, throwIfNotFound);
        }

        public static async Task<ResourceRecordSet> GetRecordSet(
        this Route53Helper r53h,
        string zoneId,
        string recordName,
        string recordType,
        string failover = null,
        bool throwIfNotFound = true)
        {
            var sets = await r53h.GetRecordSetsAsync(zoneId, recordName, recordType, failover, throwIfNotFound: throwIfNotFound);

            if (throwIfNotFound && (sets?.Length ?? 0) != 1)
                throw new Exception($"{nameof(GetRecordSet)} Failed, more then one RecordSet with Name: '{recordName}' and Type: '{recordType}' was found. [{sets.Length}]");

            return sets?.SingleOrDefault();
        }

        public static async Task<ResourceRecordSet[]> GetRecordSetsAsync(
        this Route53Helper r53h,
        string zoneId,
        string recordName,
        string recordType,
        string failover = null,
        bool throwIfNotFound = true)
        {
            var recordNameTrimmed = recordName.TrimEnd('.').TrimStartSingle("www.");
            var set = await r53h.ListResourceRecordSetsAsync(zoneId);
            set = set?.Where(x => x.Name.TrimEnd('.').TrimStartSingle("www.") == recordNameTrimmed && x.Type == recordType && (failover == null || failover == x.Failover));

            if (!throwIfNotFound && set.IsNullOrEmpty())
                return null;

            if (set?.Count() == 0)
                throw new Exception($"{nameof(GetRecordSet)} Failed, RecordSet with Name: '{recordName}' and Type: '{recordType}' was not found.");

            return set.ToArray();
        }

        public static async Task DestroyCNameRecord(this Route53Helper r53h, string zoneId, string name, string failover = null, bool throwIfNotFound = true)
        {
            var zone = await r53h.GetHostedZoneAsync(zoneId);
            var cname = $"www.{name}.{zone.HostedZone.Name.TrimEnd('.')}";
            await r53h.DestroyRecord(zoneId, cname, "CNAME", failover: failover, throwIfNotFound: throwIfNotFound);
        }

        public static async Task DestroyRecord(this Route53Helper r53h, string zoneId, string recordName, string recordType, string failover = null, bool throwIfNotFound = true)
        {
            var records = await r53h.GetRecordSetsAsync(zoneId, recordName, recordType, failover: failover, throwIfNotFound: throwIfNotFound);

            if (!throwIfNotFound && records.IsNullOrEmpty())
                return;

            foreach(var record in records)
            {
                await r53h.DeleteResourceRecordSetsAsync(zoneId, record);
                await Task.Delay(1000); //make sure request is porcessed
            }
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
        public static Task UpsertRecordAsync(this Route53Helper r53h,
            string zoneId, string Name, string Value, RRType Type, long TTL = 0,
            string failover = null,
            string healthCheckId = null,
            string setIdentifier = null) 
                => r53h.UpsertResourceRecordSetsAsync(zoneId, new ResourceRecordSet()
            {
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
                SetIdentifier = setIdentifier,
                HealthCheckId = healthCheckId
            });
    }
}
