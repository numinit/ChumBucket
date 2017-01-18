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

namespace ChumBucket.Controllers {
    [RoutePrefix("file")]
    public class FileController : Controller {
        private StorageAdapter _adapter;

        public FileController() : base() {
            var connectionString = ConfigurationManager.ConnectionStrings["StorageConnectionString"].ConnectionString;
            // TODO: figure out a better way to select an adapter. Right now, we are just 
            // commenting out the unused one below.
            this._adapter = new BlobStorageAdapter(connectionString, "files");
            //System.Diagnostics.Debug.WriteLine("Constructor");
            //this._adapter = new DLStorageAdapter("<Your Subscription ID here>", "<Your client ID here>",
            // "<Your DL Analytics account name here>", "<Your DL Storage account name here>");
        }

        [HttpPost]
        [Route("submit")]
        public ActionResult Submit() {
            try {
                var postedFile = Request.Files["upload"];
                if (postedFile == null) {
                    throw new ArgumentException("no file provided");
                }

                // Store the file
                var name = Path.GetFileName(postedFile.FileName);
                var file = new StorageFile(postedFile.InputStream, name, postedFile.ContentType);
                var uri = this._adapter.Store(file);

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
                Uri uri = new Uri(Request.QueryString["uri"]);
                StorageFile file = this._adapter.Retrieve(uri);
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
    }
}