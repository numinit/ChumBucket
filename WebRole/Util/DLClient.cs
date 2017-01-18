using Microsoft.Azure.Management.DataLake.Analytics;
using Microsoft.Azure.Management.DataLake.Analytics.Models;
using Microsoft.Azure.Management.DataLake.Store;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest;
using Microsoft.Rest.Azure.Authentication;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace WebRole.Util {
    public class DLClient {
        private string _accountName;
        private ServiceClientCredentials _creds;
        private DataLakeStoreAccountManagementClient _adlsClient;
        private DataLakeStoreFileSystemManagementClient _adlsFsClient;
        private DataLakeAnalyticsJobManagementClient _adlaJobClient;

        public string AccountName {
            get { return this._accountName; }
        }

        public DataLakeStoreFileSystemManagementClient FsClient {
            get { return this._adlsFsClient; }
        }

        public DataLakeAnalyticsJobManagementClient JobClient {
            get { return this._adlaJobClient; }
        }

        public DLClient(string accountName, string clientSecret, string subId,
                        string clientId = null, string domain = "common") {
            if (clientId == null) {
                clientId = Guid.NewGuid().ToString();
            }

            var credential = new ClientCredential(clientId, clientSecret);
            this._creds = ApplicationTokenProvider.LoginSilentAsync(domain, credential).Result;
            this._accountName = accountName;
            this._adlsClient = new DataLakeStoreAccountManagementClient(this._creds);
            this._adlsFsClient = new DataLakeStoreFileSystemManagementClient(this._creds);
            this._adlsClient.SubscriptionId = subId;
            this._adlaJobClient = new DataLakeAnalyticsJobManagementClient(this._creds);
        }
    }
}