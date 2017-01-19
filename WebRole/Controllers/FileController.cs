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
using ChumBucket.Util;
using WebRole;

namespace ChumBucket.Controllers {
    [RoutePrefix("file")]
    public class FileController : Controller {
        private StorageAdapter _blobAdapter = AzureConfig.BLOB_STORAGE;
        private DLStorageAdapter _dataLakeAdapter = AzureConfig.DL_UPLOAD;

        [HttpPost]
        [Route("submit")]
        public ActionResult Submit() {
            try {
                var adapter = this.SchemeToAdapter(Request.Form["scheme"]);
                var postedFile = Request.Files["upload"];
                if (postedFile == null) {
                    throw new ArgumentException("no file provided");
                }

                // Store the file
                var name = Path.GetFileName(postedFile.FileName);
                var file = new StorageFile(postedFile.InputStream, name, postedFile.ContentType);
                var uri = adapter.Store(file);

                // Created
                Response.StatusCode = 201;
                return Json(new {
                    result = new {
                        uri = uri.ToString()
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
                Uri uri = new Uri(Request.QueryString["uri"]);
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

        private StorageAdapter SchemeToAdapter(string scheme) {
            if (scheme == null || scheme.Equals("wasb")) {
                return this._blobAdapter;
            } else if (scheme.Equals("adl")) {
                return this._dataLakeAdapter;
            } else {
                throw new ArgumentException("invalid scheme");
            }
        }
    }
}