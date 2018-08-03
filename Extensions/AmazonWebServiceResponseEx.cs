using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AsmodatStandard.Extensions;
using AsmodatStandard.Extensions.Collections;
using Amazon.Runtime;
using System.Net;

namespace AWSWrapper.Extensions
{
    public static class AmazonWebServiceResponseEx
    {
        public static T[] EnsureSuccess<T>(this T[] responses, [System.Runtime.CompilerServices.CallerMemberName] string callerMemberName = "") where T : AmazonWebServiceResponse
            => EnsureSuccess(responses.ToIEnumerable(), callerMemberName).ToArray();

        public static async Task<T[]> EnsureSuccess<T>(this Task<T[]> responses, [System.Runtime.CompilerServices.CallerMemberName] string callerMemberName = "") where T : AmazonWebServiceResponse
            => EnsureSuccess((await responses)?.ToIEnumerable(), callerMemberName).ToArray();

        public static IEnumerable<T> EnsureSuccess<T>(this IEnumerable<T> responses, [System.Runtime.CompilerServices.CallerMemberName] string callerMemberName = "") where T : AmazonWebServiceResponse
        {
            var errors = new List<Exception>();
            foreach (var response in responses)
                if(response?.HttpStatusCode != System.Net.HttpStatusCode.OK)
                    errors.Add(new Exception($"Status code: '{response?.HttpStatusCode}', metadata: '{response?.ResponseMetadata.JsonSerialize()}'"));

            if (errors.Count > 0)
                throw new AggregateException($"'{callerMemberName}' Failed '{errors.Count}' request/s.", errors);

            return responses;
        }

        public static T EnsureSuccess<T>(this T response, [System.Runtime.CompilerServices.CallerMemberName] string callerMemberName = "") where T : AmazonWebServiceResponse
        {
            if (response?.HttpStatusCode != System.Net.HttpStatusCode.OK)
                throw new Exception($"'{callerMemberName}' Failed. Status code: '{response?.HttpStatusCode}', metadata: '{response?.ResponseMetadata.JsonSerialize()}'");

            return response;
        }

        public static async Task<T> EnsureSuccessAsync<T>(this Task<T> tResponse, [System.Runtime.CompilerServices.CallerMemberName] string callerMemberName = "") where T : AmazonWebServiceResponse
        {
            var response = await tResponse;
            if (response?.HttpStatusCode != System.Net.HttpStatusCode.OK)
                throw new Exception($"'{callerMemberName}' Failed. Status code: '{response?.HttpStatusCode}', metadata: '{response?.ResponseMetadata.JsonSerialize()}'");
            return response;
        }

        public static async Task<T> EnsureStatusCodeAsync<T>(this Task<T> tResponse, HttpStatusCode status, [System.Runtime.CompilerServices.CallerMemberName] string callerMemberName = "") where T : AmazonWebServiceResponse
        {
            var response = await tResponse;
            if (response?.HttpStatusCode != status)
                throw new Exception($"'{callerMemberName}' Failed. Expected Status code: '{status}' but was '{response?.HttpStatusCode}', metadata: '{response?.ResponseMetadata.JsonSerialize()}'");
            return response;
        }

        public static async Task<T> EnsureAnyStatusCodeAsync<T>(this Task<T> tResponse, params HttpStatusCode[] status) where T : AmazonWebServiceResponse
        {
            var response = await tResponse;
            if (!status.Any(x => x == response?.HttpStatusCode))
                throw new Exception($"Failed. Expected Status Code to be one of '{status.JsonSerialize()}' but was '{response?.HttpStatusCode}', metadata: '{response?.ResponseMetadata.JsonSerialize()}'");
            return response;
        }
    }
}
