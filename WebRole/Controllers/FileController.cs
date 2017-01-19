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

        [HttpGet]
        [Route("sas")]
        public ActionResult Sas()
        {
            const string STORAGE_ACCOUNT_NAME = "ACCOUNT NAME";
            const string STORAGE_ACCOUNT_KEY = "ACCOUNT KEY";
            var accountAndKey = new StorageCredentials(STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY);
            var blobUri = Request.QueryString.Get("bloburi");
            var verb = Request.QueryString.Get("_method");

            var sas = getSasForBlob(accountAndKey, blobUri, verb);

            byte[] buffer = System.Text.Encoding.UTF8.GetBytes(sas);
            Response.StatusCode = 200;
            //Response.ContentLength64 = buffer.Length;
            System.IO.Stream output = Response.OutputStream;
            output.Write(buffer, 0, buffer.Length);
            output.Close();
            return new HttpStatusCodeResult(200);
        }

        [HttpPost]
        [Route("success")]
        public ActionResult Success()
        {
            return new HttpStatusCodeResult(200);
        }

        private static String getSasForBlob(StorageCredentials credentials, String blobUri, String verb)
        {
            CloudBlockBlob blob = new CloudBlockBlob(new Uri(blobUri), credentials);
            var permission = SharedAccessBlobPermissions.Write;

            if (verb == "DELETE")
            {
                permission = SharedAccessBlobPermissions.Delete;
            }

            var sas = blob.GetSharedAccessSignature(new SharedAccessBlobPolicy()
            {

                Permissions = permission,
                SharedAccessExpiryTime = DateTime.UtcNow.AddMinutes(15),
            });

            return string.Format(CultureInfo.InvariantCulture, "{0}{1}", blob.Uri, sas);
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