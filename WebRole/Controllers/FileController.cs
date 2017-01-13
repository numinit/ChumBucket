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

namespace WebRole.Controllers
{
    [RoutePrefix("file")]
    public class FileController : Controller
    {
        private CloudStorageAccount _storageAccount;
        private CloudBlobClient _blobClient;
        private CloudBlobContainer _blobContainer;

        public FileController() : base()
        {
            var setting = ConfigurationManager.ConnectionStrings["StorageConnectionString"].ConnectionString;
            this._storageAccount = CloudStorageAccount.Parse(setting);
            this._blobClient = this._storageAccount.CreateCloudBlobClient();
            this._blobContainer = this._blobClient.GetContainerReference("files");
            this._blobContainer.CreateIfNotExists();
        }

        [Route("submit")]
        [HttpPost]
        public ActionResult Submit()
        {
            var file = Request.Files["upload"];
            if (file == null)
            {
                throw new ArgumentException("no file provided");
            }
            var key = this.StoreFile(file);

            // return key to client
            return Json(new {
                guid = key
            });
        }

        [Route("query")]
        [HttpGet]
        public ActionResult Query(string id)
        {
            var guid = System.Guid.Parse(id);
            this.RetrieveFile(guid, Response);
            return new EmptyResult();
        }

        private string StoreFile(HttpPostedFileBase file)
        {
            var guid = System.Guid.NewGuid();
            var key = guid.ToString();
            var blob = this._blobContainer.GetBlockBlobReference(key);
            using (file.InputStream)
            {
                blob.Properties.ContentType = file.ContentType;
                blob.UploadFromStream(file.InputStream);
            }
            return key;
        }

        private void RetrieveFile(Guid guid, HttpResponseBase response)
        {
            var key = guid.ToString();
            var blob = this._blobContainer.GetBlockBlobReference(key);
            if (!blob.Exists())
            {
                throw new ArgumentException(String.Format("blob with GUID {0} doesn't exist", key));
            }
            response.Headers["Content-Type"] = blob.Properties.ContentType;
            response.Flush();
            blob.DownloadToStream(response.OutputStream);
            response.End();
        }
    }
}