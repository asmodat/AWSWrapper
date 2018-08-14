using System.Threading.Tasks;
using System.Threading;
using Amazon.CertificateManager.Model;
using AsmodatStandard.Extensions.Collections;
using System.Linq;
using System.Collections.Generic;

namespace AWSWrapper.ACM
{
    public static class ACMHelperEx
    {
        public static async Task<CertificateDetail> DescribeCertificateByDomainName(this ACMHelper acm, string domainName, CancellationToken cancellationToken = default(CancellationToken))
        {
           var certificatesSummary = await acm.ListCertificatesAsync(cancellationToken);
           var certificateSummary = certificatesSummary.Single(x => x.DomainName == domainName);
           var cert = await acm.DescribeCertificateAsync(certificateSummary.CertificateArn, cancellationToken);
           return cert.Certificate;
        }

        public static async Task<(string Certificate, string CertificateChain)> GetCertificateByDomainName(this ACMHelper acm, string domainName, CancellationToken cancellationToken = default(CancellationToken))
        {
            var certificatesSummary = await acm.ListCertificatesAsync(cancellationToken);
            var certificateSummary = certificatesSummary.Single(x => x.DomainName == domainName);
            var cert = await acm.GetCertificateAsync(certificateSummary.CertificateArn, cancellationToken);
            return (cert.Certificate, cert.CertificateChain);
        }
    }
}
