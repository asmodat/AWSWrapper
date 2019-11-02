using AsmodatStandard.Extensions;
using AsmodatStandard.Types;
using System;

namespace AWSWrapper.S3.Models
{
    public class StatusFile
    {
        public DateTime GetDateTime() => timestamp.ToDateTimeFromUnixTimestamp();

        public string bucket { get; set; }
        public string key { get; set; }
        public string location { get; set; }
        public string id { get; set; }

        public long timestamp { get; set; }
        public ulong version { get; set; } = 0;

        public bool finalized { get; set; } = false;
        public long intensity = 0;
        public SyncTarget target { get; set; }
        public SilyFileInfo[] files { get; set; }
        public SilyDirectoryInfo[] directories { get; set; }
        public string[] obsoletes { get; set; }

        public string source { get; set; }
        public string destination { get; set; }
    }
}
