using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using AWSWrapper.Extensions;
using AsmodatStandard.Extensions;
using AsmodatStandard.Extensions.Collections;

namespace AWSWrapper.SNS
{
    public partial class SNSHelper
    {
        private readonly int _maxDegreeOfParalelism;
        internal readonly AmazonSimpleNotificationServiceClient _client;

       
        public SNSHelper(int maxDegreeOfParalelism = 2)
        {
            _maxDegreeOfParalelism = maxDegreeOfParalelism;
            _client = new AmazonSimpleNotificationServiceClient();
        }
        
    }
}
