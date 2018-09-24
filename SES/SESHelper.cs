using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SimpleEmail;
using Amazon.SimpleEmail.Model;
using AWSWrapper.Extensions;
using AsmodatStandard.Extensions;
using AsmodatStandard.Extensions.Collections;

namespace AWSWrapper.SES
{
    public partial class SESHelper
    {
        private readonly int _maxDegreeOfParalelism;
        internal readonly AmazonSimpleEmailServiceClient _client;
       
        public SESHelper(int maxDegreeOfParalelism = 2)
        {
            _maxDegreeOfParalelism = maxDegreeOfParalelism;
            _client = new AmazonSimpleEmailServiceClient();
        }

        public Task<SendEmailResponse> SendEmailAsync(
            string from,
            IEnumerable<string> to,
            string subject,
            string htmlBody,
            string textBody,
            string configurationSetName = null,
            IEnumerable<string> cc = null,
            IEnumerable<string> bcc = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            if (!textBody.IsNullOrEmpty() && !htmlBody.IsNullOrEmpty())
                throw new ArgumentException("Either text or html body must be specified, but NOT both!");

            return _client.SendEmailAsync(new SendEmailRequest()
            {

                Source = from,
                Destination = new Destination()
                {
                    ToAddresses = to.ToList(),
                    BccAddresses = bcc?.ToList(),
                    CcAddresses = cc?.ToList()
                },
                ConfigurationSetName = configurationSetName,
                Message = new Message(
                    new Content() { Charset = "UTF-8", Data = subject },
                    new Body()
                    {
                        Html = htmlBody.IsNullOrEmpty() ? null : new Content()
                        {
                            Charset = "UTF-8",
                            Data = htmlBody
                        },
                        Text = new Content()
                        {
                            Charset = "UTF-8",
                            Data = htmlBody
                        },
                    }),
            }, cancellationToken).EnsureSuccessAsync();
        }
    }
}
