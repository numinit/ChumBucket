using Microsoft.Azure.Management.DataLake.Analytics;
using Microsoft.Azure.Management.DataLake.Store;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest;
using Microsoft.Rest.Azure.Authentication;
using System;

namespace ChumBucket.Util.DataLake {
    /**
     * Encapsulates the several clients comprising a Microsoft
     * Azure Data Lake client.
     */
    public class DLClient {
        private readonly string _accountName;
        private readonly ServiceClientCredentials _creds;
        private readonly DataLakeStoreAccountManagementClient _adlsClient;
        private readonly DataLakeStoreFileSystemManagementClient _adlsFsClient;
        private readonly DataLakeAnalyticsJobManagementClient _adlaJobClient;

        public string AccountName => this._accountName;

        public DataLakeStoreFileSystemManagementClient FsClient => this._adlsFsClient;

        public DataLakeAnalyticsJobManagementClient JobClient => this._adlaJobClient;

        public DLClient(string accountName, string clientSecret, string subId,
                        string clientId, string domain) {
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