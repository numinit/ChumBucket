using ChumBucket.Util.Uris;
using System;

namespace ChumBucket.Util.DataLake {
    /**
     * Defines a convenient abstraction for job status.
     */
    public class DLJobStatus {
        private readonly JobEntityUri _uri;
        private readonly string _status;
        private readonly DateTime _startTime;
        private readonly DateTime? _endTime;
        private readonly long? _bytes;
        private readonly double? _throughput;
        private readonly string _error;

        /**
         * Returns true if this job succeeded, false otherwise.
         */
        public bool Succeeded => this._status == "SUCCEEDED";

        /**
         * Returns true if this job failed, false otherwise.
         */
        public bool Failed => this._status == "FAILED";

        /**
         * Returns true if this job neither succeeded nor failed,
         * false otherwise.
         */
        public bool InProgress => !this.Succeeded && !this.Failed;

        /**
         * Returns a JobEntityUri for this job.
         * <see cref="JobEntityUri"/>
         */
        public JobEntityUri Uri => this._uri;

        /**
         * Returns the status of this job.
         * If the status is "SUCCEEDED" or "FAILED",
         * the job is done.
         */
        public string Status => this._status;

        /**
         * Returns the start time of this job.
         */
        public DateTime StartTime => this._startTime;

        /**
         * Returns the end time of this job.
         */
        public DateTime? EndTime => this._endTime;

        /**
         * Returns this job's total duration.
         */
        public TimeSpan Duration {
            get {
                if (!this.EndTime.HasValue) {
                    return DateTime.UtcNow.Subtract(this.StartTime);
                } else {
                    return this.EndTime.Value.Subtract(this.StartTime);
                }
            }
        }

        /**
         * Returns the number of bytes this job read.
         */
        public long? Bytes => this._bytes;

        /**
         * Returns the total read throughput.
         */
        public double? Throughput => this._throughput;

        /**
         * Returns a very descriptive error string if any issues occurred 
         * during this job's execution.
         */
        public string Error => this._error;

        /**
         * Initializes this DLJobStatus.
         * <param name="uri">The URI</param>
         * <param name="status">The status string</param>
         * <param name="startTime">The start time</param>
         * <param name="endTime">The end time</param>
         * <param name="bytes">The total number of bytes read</param>
         * <param name="throughput">The total throughput</param>
         * <param name="error">The error, if present</param>
         */
        public DLJobStatus(JobEntityUri uri, string status, DateTime startTime,
                           DateTime? endTime = null,
                           long? bytes = null, double? throughput = null,
                           string error = null) {
            this._uri = uri;
            this._status = status;
            this._startTime = startTime;
            this._endTime = endTime;
            this._bytes = bytes;
            this._throughput = throughput;
            this._error = error;
        }
    }
}