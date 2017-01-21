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

namespace ChumBucket.Controllers {
    [RoutePrefix("file")]
    public class FileController : Controller {
        private BlobStorageAdapter _blobAdapter = AzureConfig.BLOB_STORAGE;
        private DLStorageAdapter _dataLakeAdapter = AzureConfig.DL_UPLOAD;

        [HttpPost]
        [Route("submit")]
        public ActionResult Submit() {
            try {
                var bucket = this.NormalizeBucketName(Request.Form["bucket"]);
                var postedFile = Request.Files["upload"];
                if (postedFile == null) {
                    throw new ArgumentException("no file provided");
                }

                // Generate a file to store
                var name = Path.GetFileName(postedFile.FileName);
                var uri = new BlobStorageEntityUri(bucket: bucket, key: name);
                var file = new StorageFile(postedFile.InputStream, uri, postedFile.ContentType);

                // Store the file
                var startTime = DateTime.UtcNow;
                this._blobAdapter.Store(file, bucket);
                var duration = DateTime.UtcNow.Subtract(startTime).Milliseconds;
                var transferRate = postedFile.ContentLength / (duration / 1000.0);

                // Done
                Response.StatusCode = 201;
                return Json(new {
                    result = new {
                        uri = uri.ToString(),
                        startTime = startTime.ToString("o"),
                        durationMs = duration,
                        transferRate = transferRate
                    }
                });
            } catch (ArgumentException e) {
                // Bad request
                Response.StatusCode = 400;
                return Json(new {
                    error = e.Message
                });
            }
        }

        [HttpGet]
        [Route("query")]
        public ActionResult Query() {
            try {
                // /query?uri={uri}
                var uri = new BlobStorageEntityUri(uri: Request.QueryString["uri"]);
                StorageFile file = this.SchemeToAdapter(uri.Scheme).Retrieve(uri);
                Response.StatusCode = 200;
                return new FileStreamResult(file.InputStream, file.ContentType);
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
        [Route("sas")]
        public ActionResult Sas() {
            try {
                System.Diagnostics.Debug.WriteLine(Request.QueryString["bloburi"]);
                var blobUri = new Uri(Request.QueryString["bloburi"]);
                var verb = Request.QueryString["_method"];

                var sas = this.GetSasForBlob(blobUri, verb);
                return new FileStreamResult(
                    new MemoryStream(Encoding.UTF8.GetBytes(sas)),
                    "application/octet-stream"
                );
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
                //var blobName = Request.Form["blob"];
                //var fileName = Request.Form["name"];
                //var guid = Guid.Parse(Request.Form["uuid"]);
                //var container = Request.Form["container"];

                //// Build a URI to the file
                //var uri = this._blobAdapter.BuildUri(guid);

                //System.Diagnostics.Debug.WriteLine();

                return new HttpStatusCodeResult(200);
            } catch (Exception e) when (e is ArgumentException || e is FormatException) {
                Response.StatusCode = 400;
                return Json(new {
                    error = e.Message
                }, JsonRequestBehavior.AllowGet);
            }
        }

        private string GetSasForBlob(Uri blobUri, string verb) {
            var credentials = this._blobAdapter.StorageAccount.Credentials;
            var blob = new CloudBlockBlob(blobUri, credentials);
            var permission = SharedAccessBlobPermissions.Write;

            if (verb == "DELETE") {
                permission = SharedAccessBlobPermissions.Delete;
            }

            // Expire their token after 15 minutes
            var sas = blob.GetSharedAccessSignature(new SharedAccessBlobPolicy() {
                Permissions = permission,
                SharedAccessExpiryTime = DateTime.UtcNow.AddMinutes(15),
            });

            return string.Format("{0}{1}", blob.Uri, sas);
        }

        /* Bucket names must start with a letter */
        static Regex BUCKET_NAME_REGEX = new Regex(@"^[a-z][a-z0-9_\-]*$");

        private string NormalizeBucketName(string name) {
            var lowerName = name.ToLowerInvariant();
            var match = BUCKET_NAME_REGEX.Match(lowerName);
            if (match.Success) {
                return lowerName;
            } else {
                throw new ArgumentException("invalid bucket name");
            }
        }

        private IStorageAdapter SchemeToAdapter(string scheme) {
            if (scheme == null || scheme == "wasb") {
                return this._blobAdapter;
            } else if (scheme == "adl") {
                return this._dataLakeAdapter;
            } else {
                throw new ArgumentException("invalid scheme");
            }
        }
    }
}