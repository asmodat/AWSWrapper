using System.Collections.Generic;
using System.Linq;
using AsmodatStandard.Extensions;
using AsmodatStandard.Extensions.Collections;

namespace AWSWrapper.S3.Models
{
    public class SyncInfo
    {
        public SyncInfo(SyncTarget st)
        {
            this.type = st.type;
            this.id = st.id;
            this.description = st.description;
        }

        public string id { get; set; }
        public SyncTarget.types type { get; private set; }
        public string description { get; set; }
        public long start { get; set; }
        public long stop { get; set; }
        public double speed { get; set; }
        public double compression { get; set; }
        public long total { get; set; }
        public long processed { get; set; }
        public long transferred { get; set; }
        public double progress { get; set; }
        public bool success { get; set; } = false;
    }

    public class SyncStatus
    {
        public SyncStatus(Dictionary<string, SyncInfo> infos, Dictionary<string,SyncResult> results, long run)
        {
            this.run = run;
            this._infos = infos?.Where(x => x.Value != null).ToDictionary(x => x.Key, x => x.Value);
            this.results = results?.Where(x => x.Value != null).ToDictionary(x => x.Key, x => x.Value);
        }

        public long run { get; set; }

        public long targets { get => _infos?.Count() ?? 0; }

        public double progress
        {
            get
            {
                if (_infos.IsNullOrEmpty())
                    return double.NaN;

                var sum = _infos.Values.Sum(x => x.progress);
                var progress = sum / _infos.Count();
                return progress == 0 ? double.NaN : progress;
            }
        }

        private Dictionary<string, SyncInfo> _infos { get; set; }
        public Dictionary<string, SyncResult> results { get; set; }
    }
}
