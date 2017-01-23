using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Web.Mvc;
using WebRole;
using System.Security;
using ChumBucket.Util.Storage;
using ChumBucket.Util.Uris;
using System.IO.Compression;
using System.Web;
using MimeTypes;

namespace ChumBucket.Controllers {
    [RoutePrefix("file")]
    public class FileController : Controller {
        private BlobStorageAdapter _blobAdapter = AzureConfig.BLOB_STORAGE;

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
        [Route("getDirectUri")]
        public ActionResult GetDirectUri() {
            try {
                // Give them back a direct SAS URI.
                var uri = new BlobStorageEntityUri(uri: Request.QueryString["uri"]);
                var sasUri = this._blobAdapter.GetSasForBlob(uri, "GET", new[] {"GET"}, 60);

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
            } catch (SecurityException e) {
                // Forbidden
                Response.StatusCode = 403;
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

        [HttpDelete]
        [Route("delete")]
        public ActionResult Delete() {
            try {
                // Give them back a direct SAS URI.
                var uri = new BlobStorageEntityUri(uri: Request.QueryString["uri"]);
                this._blobAdapter.Delete(uri);

                Response.StatusCode = 202;
                return Json(new {
                    result = new {
                        deletedUri = uri.ToString()
                    }
                }, JsonRequestBehavior.AllowGet);
            } catch (Exception e) when (e is ArgumentException || e is FormatException) {
                // Bad request
                Response.StatusCode = 400;
                return Json(new {
                    error = e.Message
                }, JsonRequestBehavior.AllowGet);
            } catch (SecurityException e) {
                // Forbidden
                Response.StatusCode = 403;
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
        [Route("uploadSignature")]
        public ActionResult UploadSignature() {
            try {
                var blobUri = new Uri(Request.QueryString["bloburi"]);
                var verb = Request.QueryString["_method"];
                if (verb == null) {
                    throw new ArgumentException("no verb provided");
                }

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
        [Route("uploadSuccess")]
        public ActionResult UploadSuccess() {
            try {
                var container = Request.Form["container"];
                var blobName = Request.Form["blob"];
                var fileName = Request.Form["name"];

                if (container == null || blobName == null || fileName == null ||
                    container.Length == 0 || blobName.Length == 0 || fileName.Length == 0) {
                    throw new ArgumentException("container, blob, and name must be provided");
                }

                // Build a request/result URI
                var mimeType = MimeTypeMap.GetMimeType(Path.GetExtension(blobName));
                var requestUri = new Uri($"{container}/{blobName}");
                var resultUri = this._blobAdapter.FinalizeUploadedBlob(requestUri, fileName, mimeType);

                // Unzip if necessary
                if (mimeType == "application/zip") {
                    this.Unzip(resultUri);
                }

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

        private void Unzip(BlobStorageEntityUri uri) {
            var file = this._blobAdapter.Retrieve(uri);
            using (var zip = new ZipArchive(file.InputStream)) {
                foreach (var entry in zip.Entries) {
                    var filename = entry.Name;
                    if (filename.EndsWith(".csv")) {
                        // Extract it to the same bucket
                        var zipEntryUri = new BlobStorageEntityUri(bucket: uri.Bucket, key: filename);
                        var storageFile = new StorageFile(entry.Open(), zipEntryUri, "text/csv");

                        // Store it
                        this._blobAdapter.Store(storageFile, zipEntryUri.Bucket);
                    }
                }
            }
        }
    }
}