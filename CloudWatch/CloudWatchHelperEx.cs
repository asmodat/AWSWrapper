using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon;
using Amazon.ECS;
using AsmodatStandard.Extensions;
using AsmodatStandard.Threading;
using AsmodatStandard.Extensions.Collections;
using System.Threading;
using Amazon.CloudWatch.Model;
using AWSWrapper.ELB;
using static AWSWrapper.CloudWatch.CloudWatchHelper;
using Amazon.CloudWatch;
using System.Diagnostics;

namespace AWSWrapper.CloudWatch
{
    public static class CloudWatchHelperEx
    {
        public static async Task<StateValue> WaitForMetricState(this CloudWatchHelper cwh, string name, StateValue stateValue, int timeout_s)
        {
            var sw = Stopwatch.StartNew();
            MetricAlarm ma;
            do
            {
                ma = await cwh.GetMetricAlarmAsync(name, throwIfNotFound: true);

                if (ma.StateValue == stateValue)
                    return ma.StateValue;

                await Task.Delay(1000);
            }
            while (sw.ElapsedMilliseconds < (timeout_s * 1000));

            throw new Exception($"Metric '{name}' coudn't reach '{stateValue}' state within {timeout_s} [s], last state was: '{ma.StateValue}'.");
        }

        public static async Task<MetricAlarm> UpsertAELBMetricAlarmAsync(
            this CloudWatchHelper cwh, ELBHelper elb,
            string name,
            string loadBalancer,
            string targetGroup,
            ELBMetricName metric,
            ComparisonOperator comparisonOperator,
            int treshold,
            Statistic statistic = null,
            int dataPointToAlarm = 1,
            int evaluationPeriod = 1,
            int requestDelay = 1000,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var alb = (await elb.GetLoadBalancersByName(loadBalancer, throwIfNotFound: true)).Single();
            var tg = await elb.GetTargetGroupByName(targetGroup, alb, throwIfNotFound: true);

            var loadBalanceValue = alb.LoadBalancerArn.SplitByLast(':')[1].TrimStartSingle("loadbalancer/");
            var targetGroupValue = tg.TargetGroupArn.SplitByLast(':')[1];
            var stat = statistic != null ? statistic :
                (comparisonOperator == ComparisonOperator.GreaterThanOrEqualToThreshold || comparisonOperator == ComparisonOperator.GreaterThanThreshold)
                ? Statistic.Maximum : Statistic.Minimum;

            var deleteAlarm = await cwh.DeleteMetricAlarmAsync(name, throwIfNotFound: false);

            if(deleteAlarm.HttpStatusCode != System.Net.HttpStatusCode.NotFound)
                await Task.Delay(requestDelay); //lets ensure metric was removed before we push new request

            var mar = await cwh.PutAELBMetricAlarmAsync(
                name, loadBalanceValue, targetGroupValue, 
                metric: metric,
                comparisonOperator: comparisonOperator,
                statistic: stat,
                treshold: treshold,
                dataPointToAlarm: dataPointToAlarm,
                evaluationPeriod: evaluationPeriod,
                cancellationToken: cancellationToken);

            await Task.Delay(requestDelay); //lets ensure metric exists before we search for it

            return await cwh.GetMetricAlarmAsync(name: name, throwIfNotFound: true, cancellationToken: cancellationToken);
        }

        public static async Task UpsertEcsAliveMetricAlarmAsync(
            this CloudWatchHelper cwh,
            string name,
            string clusterName,
            string serviceName,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            await cwh.DeleteMetricAlarmAsync(name, throwIfNotFound: false);
            await cwh.PutEcsAliveMetricAlarmAsync(name, clusterName, serviceName, cancellationToken);
        }

        public static async Task<MetricAlarm> GetMetricAlarmAsync(this CloudWatchHelper cwh,
            string name,
            bool throwIfNotFound = true,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var alarms = await cwh.ListMetricAlarmsAsync(alarmNamePrefix: name, cancellationToken: cancellationToken);
            var alarm = alarms.SingleOrDefault(x => x.AlarmName == name);
            if (throwIfNotFound && alarm == null)
                throw new Exception($"MetricAlarm with name: {name} was not found.");

            return alarm;
        }

        public static async Task<DeleteAlarmsResponse> DeleteMetricAlarmAsync(this CloudWatchHelper cwh,
            string name,
            bool throwIfNotFound,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var alarms = await cwh.ListMetricAlarmsAsync(alarmNamePrefix: name, cancellationToken: cancellationToken);
            var alarm = alarms.SingleOrDefault(x => x.AlarmName == name);
            if (throwIfNotFound && alarm == null)
                throw new Exception($"MetricAlarm with name: {name} was not found and can't be deleted.");

            if (alarm == null)
                return new DeleteAlarmsResponse() { HttpStatusCode = System.Net.HttpStatusCode.NotFound };

            return await cwh.DeleteAlarmAsync(alarm.AlarmName, cancellationToken);
        }

        public static Task DeleteLogGroupAsync(this CloudWatchHelper cwh, 
            string name, 
            bool throwIfNotFound = true, 
            CancellationToken cancellationToken = default(CancellationToken))
                => cwh.DeleteLogGroupsAsync(new string[] { name }, throwIfNotFound, cancellationToken);
    }
}
