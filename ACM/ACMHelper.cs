﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.CertificateManager;
using Amazon.CertificateManager.Model;
using AWSWrapper.Extensions;
using AsmodatStandard.Extensions;
using AsmodatStandard.Extensions.Collections;

namespace AWSWrapper.ACM
{
    public partial class ACMHelper
    {
        internal readonly int _maxDegreeOfParalelism;
        internal readonly AmazonCertificateManagerClient _client;

        public ACMHelper(int maxDegreeOfParalelism = 8)
        {
            _maxDegreeOfParalelism = maxDegreeOfParalelism;
            _client = new AmazonCertificateManagerClient();
        }

        public async Task<CertificateSummary[]> ListCertificatesAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            string nextToken = null;
            ListCertificatesResponse response;
            var results = new List<CertificateSummary>();
            while ((response = await _client.ListCertificatesAsync(new ListCertificatesRequest()
            {
                NextToken = nextToken,
                MaxItems = 1000
            }, cancellationToken).EnsureSuccessAsync()) != null)
            {
                if ((response?.CertificateSummaryList?.Count ?? 0) == 0)
                    break;

                results.AddRange(response.CertificateSummaryList);
              
                if (response.NextToken.IsNullOrEmpty())
                    break;

                nextToken = response.NextToken;
            }

            return results.ToArray();
        }

        public Task<DescribeCertificateResponse> DescribeCertificateAsync(string arn, CancellationToken cancellationToken = default(CancellationToken))
            => _client.DescribeCertificateAsync(new DescribeCertificateRequest() {
                CertificateArn = arn
            }, cancellationToken).EnsureSuccessAsync();

        public Task<GetCertificateResponse> GetCertificateAsync(string arn, CancellationToken cancellationToken = default(CancellationToken))
            => _client.GetCertificateAsync(new GetCertificateRequest()
            {
                CertificateArn = arn
            }, cancellationToken).EnsureSuccessAsync();
    }
}
