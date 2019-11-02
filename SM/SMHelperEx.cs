using System.Threading.Tasks;
using System.Threading;
using AsmodatStandard.Extensions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace AWSWrapper.SM
{
    public static class SMHelperEx
    {
        public static async Task<string> GetSecret(this SMHelper client, string name, string key, string versionStage = "AWSCURRENT", CancellationToken cancellationToken = default(CancellationToken))
        {
            var json = await client.GetSecret(name: name, versionStage: versionStage, cancellationToken: cancellationToken);
            var data = (JObject)JsonConvert.DeserializeObject(json);

            if (key.IsNullOrEmpty())
                return data.JsonSerialize(formatting: Formatting.Indented);

            if (data?.ContainsKey(key) != true)
                throw new System.Exception($"Key '{key ?? "undefined"}' was not found for the secret '{name ?? "undefined"}'");

            return data[key].Value<string>();
        }



        
    }
}
