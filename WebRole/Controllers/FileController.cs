using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using System.IO;
using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure;
using System.Configuration;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Shared.Protocol;
using ChumBucket.Util;
using WebRole;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using System.Security;

namespace ChumBucket.Controllers {
    [RoutePrefix("file")]
    public class FileController : Controller {
        private BlobStorageAdapter _blobAdapter = AzureConfig.BLOB_STORAGE;

        [HttpGet]
        [Route("query")]
        public ActionResult Query() {
            try {
                // Give them back a direct SAS URI.
                var uri = new BlobStorageEntityUri(uri: Request.QueryString["uri"]);
                var sasUri = this._blobAdapter.GetSasForBlob(uri, Request.HttpMethod, new[] {"GET"}, 60);

                Response.StatusCode = 200;
                return Json(new {
                    result = new {
                        uri = sasUri.ToString()
                    }
                }, JsonRequestBehavior.AllowGet);
            } catch (Exception e) when (e is ArgumentException || e is FormatException) {
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
        [Route("listBuckets")]
        public ActionResult ListBuckets() {
            var buckets = this._blobAdapter.ListBuckets().Select(x => x.ToString());

            Response.StatusCode = 200;
            return Json(new {
                result = new {
                    uris = buckets
                }
            }, JsonRequestBehavior.AllowGet);
        }

        [HttpGet]
        [Route("listFilesInBucket")]
        public ActionResult ListFilesInBucket() {
            try {
                var uri = new BlobStorageEntityUri(uri: Request.QueryString["uri"]);
                var files = this._blobAdapter.ListFiles(uri.Bucket).Select(x => x.ToString());

                Response.StatusCode = 200;
                return Json(new {
                    result = new {
                        uris = files
                    }
                }, JsonRequestBehavior.AllowGet);
            } catch (Exception e) when (e is ArgumentException || e is FormatException) {
                // Bad request
                Response.StatusCode = 400;
                return Json(new {
                    error = e.Message
                }, JsonRequestBehavior.AllowGet);
            }
        }

        [HttpGet]
        [Route("sas")]
        public ActionResult Sas() {
            try {
                var blobUri = new Uri(Request.QueryString["bloburi"]);
                var verb = Request.QueryString["_method"];

                var sasUri = this._blobAdapter.GetSasForBlob(blobUri, verb, new[] {"PUT", "DELETE"});
                Response.StatusCode = 200;
                return Content(sasUri.ToString(), "text/plain");
            } catch (SecurityException e) {
                // Forbidden
                Response.StatusCode = 403;
                return Json(new {
                    error = e.Message
                }, JsonRequestBehavior.AllowGet);
            } catch (Exception e) when (e is ArgumentException || e is FormatException) {
                // Bad request
                Response.StatusCode = 400;
                return Json(new {
                    error = e.Message
                }, JsonRequestBehavior.AllowGet);
            }
        }

        [HttpPost]
        [Route("success")]
        public ActionResult Success() {
            try {
                var container = Request.Form["container"];
                var blobName = Request.Form["blob"];
                var fileName = Request.Form["name"];

                // Added in the `params` object client-side.
                var mimeType = Request.Form["mimeType"];

                // Build a request/result URI
                var requestUri = new Uri(string.Format("{0}/{1}", container, blobName));
                var resultUri = this._blobAdapter.FinalizeUploadedBlob(requestUri, fileName, mimeType);

                Response.StatusCode = 200;
                return Json(new {
                    result = new {
                        uri = resultUri.ToString()
                    }
                });
            } catch (SecurityException e) {
                // Forbidden
                Response.StatusCode = 403;
                return Json(new {
                    error = e.Message
                });
            } catch (Exception e) when (e is ArgumentException || e is FormatException) {
                Response.StatusCode = 400;
                return Json(new {
                    error = e.Message
                });
            }
        }
    }
}