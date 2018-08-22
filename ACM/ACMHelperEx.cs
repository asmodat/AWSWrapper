using System.Threading.Tasks;
using System.Threading;
using Amazon.CertificateManager.Model;
using AsmodatStandard.Extensions.Collections;
using System.Linq;
using System.Collections.Generic;
using AWSWrapper.Route53;
using AsmodatStandard.Threading;
using Amazon.CertificateManager;

namespace AWSWrapper.ACM
{
    public static class ACMHelperEx
    {
        public static async Task<CertificateDetail> DescribeCertificateByDomainName(this ACMHelper acm, string domainName, CancellationToken cancellationToken = default(CancellationToken))
        {
            var certificatesSummary = await acm.ListCertificatesAsync(cancellationToken);
            var named = certificatesSummary.Where(x => x.DomainName == domainName);
            var details = await named.ForEachAsync(cs => acm.DescribeCertificateAsync(cs.CertificateArn, cancellationToken), 
                acm._maxDegreeOfParalelism, cancellationToken);

            return details.OrderByDescending(x => x.Certificate.IssuedAt)
                .FirstOrDefault(x => x.Certificate.Status == CertificateStatus.ISSUED)?.Certificate;
        }

        public static async Task<(string Certificate, string CertificateChain)> GetCertificateByDomainName(this ACMHelper acm, string domainName, CancellationToken cancellationToken = default(CancellationToken))
        {
            var cd = await acm.DescribeCertificateByDomainName(domainName, cancellationToken);
            var cert = await acm.GetCertificateAsync(cd.CertificateArn, cancellationToken);
            return (cert.Certificate, cert.CertificateChain);
        }
    }
}
