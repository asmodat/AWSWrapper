
namespace AWSWrapper.S3.Models
{
    public class SyncResult
    {
        public SyncResult(bool success = false, double speed = double.NaN, long duration = 0, string error = null, long run = 0)
        {
            this.run = run;
            this.success = success;
            this.speed = speed;
            this.duration = duration;
            this.error = error;
        }

        public long run { get; set; }
        public bool success { get; set; }
        public double speed { get; set; }
        public long duration { get; set; }
        public string error { get; set; }
    }
}
