using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Microsoft.Rest.Azure.Authentication;
using Microsoft.Azure.Management.DataLake.Store;
using Microsoft.Azure.Management.DataLake.StoreUploader;

namespace WebRole.Util {
    public class DataLakeStorageAdapter : StorageAdapter {
        public StorageFile Retrieve(string key) {
            throw new NotImplementedException();
        }

        public string Store(StorageFile file) {
            throw new NotImplementedException();
        }
    }
}