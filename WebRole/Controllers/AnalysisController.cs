using ChumBucket.Util;
using Microsoft.Azure.Management.DataLake.Analytics.Models;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using WebRole.Util;

namespace WebRole.Controllers {
    [RoutePrefix("analysis")]
    public class AnalysisController : Controller {
        private StorageAdapter _upload = AzureConfig.DL_UPLOAD;
        private StorageAdapter _result = AzureConfig.DL_RESULT;
        private DLJobAdapter _job = AzureConfig.DL_JOB;

        private class SubmitRequest {
            [JsonProperty("uri")]
            public string Uri { get; set; }

            [JsonProperty("name")]
            public string Name { get; set; }

            [JsonProperty("code")]
            public string Code { get; set; }
        }

        private class JobStatusResult {
            [JsonProperty("status")]
            public string Status { get; set; }

            [JsonProperty("startTime")]
            public string StartTime { get; set; }

            [JsonProperty("durationMs")]
            public int Duration { get; set; }



            [JsonProperty("result")]
            public string Result { get; set; }

            public bool Success {
                get {
                    return this.Status.Equals("SUCCEEDED");
                }
            }
        }

        [HttpPost]
        [Route("submit")]
        public ActionResult Submit() {
            try {
                string json;
                using (var reader = new StreamReader(Request.InputStream)) {
                    json = reader.ReadToEnd();
                }
                var request = JsonConvert.DeserializeObject<SubmitRequest>(json);
                var jobUri = this._job.SubmitJob(request.Name, new Uri(request.Uri), request.Code);

                Response.StatusCode = 201;
                return Json(new {
                    result = new {
                        uri = jobUri.ToString()
                    }
                });
            } catch (Exception e) when (e is ArgumentException || e is FormatException || e is JsonException) {
                // Bad request
                Response.StatusCode = 400;
                return Json(new {
                    error = e.Message
                });
            }
        }

        [HttpGet]
        [Route("status")]
        public ActionResult Status() {
            try {
                var uri = new Uri(Request.QueryString["uri"]);
                var job = this._job.GetJobInfo(uri);

                Response.StatusCode = 200;
                return this.JobToResult(job);
            } catch (Exception e) when (e is ArgumentException || e is FormatException || e is JsonException) {
                // Bad request
                Response.StatusCode = 400;
                return Json(new {
                    error = e.Message
                }, JsonRequestBehavior.AllowGet);
            } catch (KeyNotFoundException e) {
                // Not found
                Response.StatusCode = 404;
                return Json(new {
                    error = e.Message
                }, JsonRequestBehavior.AllowGet);
            }
        }

        [HttpGet]
        [Route("result")]
        public ActionResult Result() {
            try {
                Uri uri = new Uri(Request.QueryString["uri"]);
                var job = this._job.GetJobInfo(uri);
                if (job.Result == JobResult.Succeeded) {
                    var file = this._job.GetJobResult(uri);
                    Response.StatusCode = 200;
                    return new FileStreamResult(file.InputStream, file.ContentType);
                } else {
                    throw new KeyNotFoundException("job is still in progress");
                }
            } catch (Exception e) when (e is ArgumentException || e is FormatException || e is JsonException) {
                // Bad request
                Response.StatusCode = 400;
                return Json(new {
                    error = e.Message
                }, JsonRequestBehavior.AllowGet);
            } catch (KeyNotFoundException e) {
                // Not found
                Response.StatusCode = 404;
                return Json(new {
                    error = e.Message
                }, JsonRequestBehavior.AllowGet);
            }
        }

        private JsonResult JobToResult(JobInformation info) {
            var state = info.State.Value;
            var statusString = "UNKNOWN";
            var startTime = info.SubmitTime.Value;
            var startTimeString = startTime.ToString("o");
            int duration = 0;
            if (state == JobState.Ended) {
                statusString = info.Result.ToString().ToUpper();
                duration = (int)Math.Round(info.EndTime.Value.Subtract(startTime).TotalMilliseconds);
            } else {
                statusString = state.ToString().ToUpper();
                duration = (int)Math.Round(DateTimeOffset.Now.Subtract(startTime).TotalMilliseconds);
            }

            if (info.Result != JobResult.Failed) {
                return Json(new {
                    result = new {
                        status = statusString,
                        startTime = startTimeString,
                        durationMs = duration
                    }
                }, JsonRequestBehavior.AllowGet);
            } else {
                var errorMessages = new List<string>();
                foreach (var error in info.ErrorMessage) {
                    // super annoying :(
                    errorMessages.Add(string.Format("Message: {0}\nDescription: {1}\nDetails: {2}", error.Message, error.Description, error.Details));
                }
                return Json(new {
                    result = new {
                        status = statusString,
                        startTime = startTimeString,
                        duration = duration,
                        errorMessages = errorMessages
                    }
                }, JsonRequestBehavior.AllowGet);
            }
        }
    }
}