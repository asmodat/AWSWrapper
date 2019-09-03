using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.CertificateManager;
using Amazon.CertificateManager.Model;
using AWSWrapper.Extensions;
using AsmodatStandard.Extensions;
using AsmodatStandard.Extensions.Collections;
using Amazon.Runtime;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;

namespace AWSWrapper.ACM
{
    public partial class ACMHelper
    {
        internal readonly int _maxDegreeOfParalelism;
        internal readonly AmazonCertificateManagerClient _client;

        public ACMHelper(int maxDegreeOfParalelism = 2)
        {
            _maxDegreeOfParalelism = maxDegreeOfParalelism;
            _client = new AmazonCertificateManagerClient();
        }

        public async Task<CertificateSummary[]> ListCertificatesAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            ListCertificatesResponse response = null;
            var results = new List<CertificateSummary>();
            while ((response = await _client.ListCertificatesAsync(new ListCertificatesRequest()
            {
                NextToken = response?.NextToken,
                MaxItems = 1000
            }, cancellationToken).EnsureSuccessAsync()) != null)
            {
                if (!response.CertificateSummaryList.IsNullOrEmpty())
                    results.AddRange(response.CertificateSummaryList);
              
                if (response.NextToken.IsNullOrEmpty())
                    break;

                await Task.Delay(100);
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
