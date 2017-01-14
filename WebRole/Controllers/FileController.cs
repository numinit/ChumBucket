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
using WebRole.Util;

namespace WebRole.Controllers {
    [RoutePrefix("file")]
    public class FileController : Controller {
        private StorageAdapter _adapter;

        public FileController() : base() {
            var connectionString = ConfigurationManager.ConnectionStrings["StorageConnectionString"].ConnectionString;
            this._adapter = new BlobStorageAdapter(connectionString, "files");
        }

        [Route("submit")]
        [HttpPost]
        public ActionResult Submit() {
            try {
                var postedFile = Request.Files["upload"];
                if (postedFile == null) {
                    throw new ArgumentException("no file provided");
                }

                // Store the file
                var name = Path.GetFileNameWithoutExtension(postedFile.FileName);
                var file = new StorageFile(postedFile.InputStream, name, postedFile.ContentType);
                var key = this._adapter.Store(file);

                // Created
                Response.StatusCode = 201;
                return Json(new {
                    result = new {
                        key = key
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

        [Route("query")]
        [HttpGet]
        public ActionResult Query(string id) {
            try {
                StorageFile file = this._adapter.Retrieve(id);
                Response.StatusCode = 200;
                return new FileStreamResult(file.InputStream, file.ContentType);
            } catch (ArgumentException e) {
                // Bad request
                Response.StatusCode = 400;
                return Json(new {
                    error = e.Message
                });
            } catch (KeyNotFoundException e) {
                // Not found
                Response.StatusCode = 404;
                return Json(new {
                    error = e.Message
                });
            }
        }
    }
}