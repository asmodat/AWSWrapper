using Amazon.Runtime;

namespace AWSWrapper.Extensions
{
    public static class AmazonServiceClientEx
    {
        public static string GetRegionName<T>(this T client) where T : AmazonServiceClient
            => client.Config.RegionEndpoint.SystemName;
    }
}
