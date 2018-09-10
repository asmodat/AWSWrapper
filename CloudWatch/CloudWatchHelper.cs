﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon;
using Amazon.CloudWatch;
using Amazon.CloudWatchLogs;
using AsmodatStandard.Extensions;
using AsmodatStandard.Threading;
using AsmodatStandard.Extensions.Collections;
using AWSWrapper.Extensions;
using System.Threading;
using Amazon.CloudWatchLogs.Model;
using System.Net;
using Amazon.CloudWatch.Model;

namespace AWSWrapper.CloudWatch
{
    public partial class CloudWatchHelper
    {
        private readonly int _maxDegreeOfParalelism;
        private readonly AmazonCloudWatchClient _client;
        private readonly AmazonCloudWatchLogsClient _clientLogs;

        public enum ELBMetricName
        {
            HealthyHostCount = 1,
            UnHealthyHostCount = 2,
        }

        public CloudWatchHelper(int maxDegreeOfParalelism = 8)
        {
            _maxDegreeOfParalelism = maxDegreeOfParalelism;
            _client = new AmazonCloudWatchClient();
            _clientLogs = new AmazonCloudWatchLogsClient();
        }

        public Task PutEcsAliveMetricAlarmAsync(
            string name,
            string clusterName,
            string serviceName,
            CancellationToken cancellationToken = default(CancellationToken))
            => _client.PutMetricAlarmAsync(new Amazon.CloudWatch.Model.PutMetricAlarmRequest()
            {
                AlarmName = name,
                Period = 60,
                Namespace = "AWS/ECS",
                Dimensions = new List<Amazon.CloudWatch.Model.Dimension>()
                {
                    new Amazon.CloudWatch.Model.Dimension()
                    {
                        Name = "ClusterName",
                        Value = clusterName
                    },
                    new Amazon.CloudWatch.Model.Dimension()
                    {
                        Name = "ServiceName",
                        Value = serviceName
                    }
                },
                MetricName = "MemoryUtilization",
                EvaluationPeriods = 1,
                DatapointsToAlarm = 1,
                ComparisonOperator = "LessThanOrEqualToThreshold",
                ActionsEnabled = true,
                Statistic = "Average",
                Threshold = 1,
                TreatMissingData = "breaching",
                AlarmDescription = "Auto Generated by Asmodat AWSWrapper Toolkit."
            }, cancellationToken).EnsureSuccessAsync();

        public Task<PutMetricAlarmResponse> PutAELBMetricAlarmAsync(
            string name,
            string loadBalancer,
            string targetGroup,
            ELBMetricName metric,
            ComparisonOperator comparisonOperator,
            Statistic statistic,
            int treshold,
            int dataPointToAlarm = 1,
            int evaluationPeriod = 1,
            CancellationToken cancellationToken = default(CancellationToken))
            => _client.PutMetricAlarmAsync(new PutMetricAlarmRequest()
            {
                AlarmName = name,
                Period = 60,
                Namespace = "AWS/ApplicationELB",
                Dimensions = new List<Dimension>()
                {
                    new Dimension()
                    {
                        Name = "LoadBalancer",
                        Value = loadBalancer //app/test-1-ui-1-alb-pub/0d603d7ab786c184
                    },
                    new Dimension()
                    {
                        Name = "TargetGroup",
                        Value = targetGroup //"targetgroup/test-1-ui-1-tg-public/a74c4ffa6cb637ab"
                    }
                },
                MetricName = metric.ToString(),
                EvaluationPeriods = evaluationPeriod,
                DatapointsToAlarm = dataPointToAlarm,
                ComparisonOperator = comparisonOperator,
                ActionsEnabled = true,
                Statistic = statistic,
                Threshold = treshold,
                AlarmDescription = "Auto Generated by Asmodat AWSWrapper Toolkit."
            }, cancellationToken).EnsureSuccessAsync();

        public Task<DeleteAlarmsResponse> DeleteAlarmAsync(
            string name,
            CancellationToken cancellationToken = default(CancellationToken))
            => _client.DeleteAlarmsAsync(new Amazon.CloudWatch.Model.DeleteAlarmsRequest()
            {
                AlarmNames = new List<string>() { name }
            }, cancellationToken).EnsureSuccessAsync();

        public async Task<IEnumerable<Metric>> ListHealthChecksAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            var list = new List<Metric>();
            ListMetricsResponse response = null;
            while ((response = await _client.ListMetricsAsync(new ListMetricsRequest() {
                NextToken = response?.NextToken
            },cancellationToken))?.HttpStatusCode == System.Net.HttpStatusCode.OK)
            {
                if ((response.Metrics?.Count ?? 0) != 0)
                    list.AddRange(response.Metrics);
                else
                    break;
                
                if (response.NextToken.IsNullOrEmpty())
                    break;
            }

            response.EnsureSuccess();
            return list;
        }

        public async Task<IEnumerable<MetricAlarm>> ListMetricAlarmsAsync(
            string alarmNamePrefix = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var list = new List<MetricAlarm>();
            DescribeAlarmsResponse response = null;
            while ((response = await _client.DescribeAlarmsAsync(new DescribeAlarmsRequest()
            {
                NextToken = response?.NextToken,
                MaxRecords = 100,
                AlarmNamePrefix = alarmNamePrefix
            }, cancellationToken))?.HttpStatusCode == System.Net.HttpStatusCode.OK)
            {
                if ((response.MetricAlarms?.Count ?? 0) != 0)
                    list.AddRange(response.MetricAlarms);
                else
                    break;

                if (response.NextToken.IsNullOrEmpty())
                    break;
            }

            response.EnsureSuccess();
            return list;
        }

        public Task CreateLogGroupAsync(string name, CancellationToken cancellationToken = default(CancellationToken))
            => _clientLogs.CreateLogGroupAsync(new CreateLogGroupRequest()
            {
                LogGroupName = name,
                Tags = new Dictionary<string, string>() { { "Timestamp", DateTime.UtcNow.ToRfc3339String() } }
            }, cancellationToken).EnsureSuccessAsync();

        public async Task<DeleteLogGroupResponse> DeleteLogGroupAsync(string name, bool throwIfNotFound = true, CancellationToken cancellationToken = default(CancellationToken))
        {
            try
            {
                var result = (throwIfNotFound) ?
                    _clientLogs.DeleteLogGroupAsync(new DeleteLogGroupRequest() { LogGroupName = name }, cancellationToken) :
                    _clientLogs.DeleteLogGroupAsync(new DeleteLogGroupRequest() { LogGroupName = name });

                return await result.EnsureSuccessAsync();
            }
            catch (Amazon.CloudWatchLogs.Model.ResourceNotFoundException ex)
            {
                if (throwIfNotFound)
                    throw ex;

                return new DeleteLogGroupResponse() { HttpStatusCode = HttpStatusCode.NotFound };
            }
        }

        public Task<DeleteLogGroupResponse[]> DeleteLogGroupsAsync(IEnumerable<string> names, bool throwIfNotFound = true, CancellationToken cancellationToken = default(CancellationToken))
            => names.ForEachAsync(name => DeleteLogGroupAsync(name, throwIfNotFound: throwIfNotFound, cancellationToken: cancellationToken), _maxDegreeOfParalelism, cancellationToken)
            .EnsureSuccess();
    }
}
