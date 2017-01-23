using ChumBucket.Util;
using Microsoft.Azure.Management.DataLake.Analytics.Models;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Web;
using System.Web.Mvc;
using ChumBucket.Util.DataLake;
using ChumBucket.Util.Storage;
using ChumBucket.Util.Uris;

namespace WebRole.Controllers {
    [RoutePrefix("analysis")]
    public class AnalysisController : Controller {
        private readonly DLJobAdapter _job = AzureConfig.DL_JOB;

        /**
         * A job submission request
         */
        private class SubmitRequest {
            [JsonProperty("name")]
            public string Name { get; set; }

            [JsonProperty("code")]
            public string Code { get; set; }
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
                if (request == null) {
                    throw new ArgumentException("invalid request");
                }
                var jobUri = this._job.SubmitJob(request.Name, request.Code);

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
                var uri = new JobEntityUri(uri: Request.QueryString["uri"]);
                var job = this._job.GetJobStatus(uri);

                Response.StatusCode = 200;
                return Json(new {
                    result = new {
                        uri = job.Uri.ToString(),
                        status = job.Status,
                        startTime = job.StartTime.ToString("o"),
                        durationMs = (int)Math.Floor(job.Duration.TotalMilliseconds),
                        dataReadBytes = job.Succeeded ? job.Bytes.Value : -1,
                        throughputBytesPerSecond = job.Succeeded ? job.Throughput.Value : -1.0,
                        error = job.Failed ? job.Error : null
                    }
                }, JsonRequestBehavior.AllowGet);
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
                var uri = new JobEntityUri(uri: Request.QueryString["uri"]);
                var job = this._job.GetJobStatus(uri);
                if (job.Succeeded) {
                    var file = this._job.GetJobResult(uri);
                    Response.StatusCode = 200;

                    // Force the file to appear in the web browser with text/plain content type
                    return new FileStreamResult(file.InputStream, "text/plain");
                } else {
                    throw new KeyNotFoundException("job is unfinished");
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
    }
}