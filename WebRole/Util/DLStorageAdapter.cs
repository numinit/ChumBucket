using System;
using System.IO;
using System.Collections.Generic;
using System.Threading;
using Microsoft.Rest;
using Microsoft.Rest.Azure.Authentication;
using Microsoft.Azure.Management.DataLake.Store;
using Microsoft.Azure.Management.DataLake.StoreUploader;
using Microsoft.Azure.Management.DataLake.Analytics;
using Microsoft.Azure.Management.DataLake.Analytics.Models;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Text;

namespace WebRole.Util
{
    public class DLStorageAdapter : StorageAdapter {
        private string _subscriptionId;
        private string _clientId;
        private string _domainName; // Replace this string with the user's Azure Active Directory tenant ID or domain name, if needed.

        private string _adlaAccountName;
        private string _adlsAccountName;

        private DataLakeAnalyticsAccountManagementClient _adlaClient;
        private DataLakeStoreFileSystemManagementClient _adlsFileSystemClient;
        private DataLakeAnalyticsJobManagementClient _adlaJobClient;

        public DLStorageAdapter(string subscriptionId, string clientId, 
            string adlaAccountName, string adlsAccountName, string domainName = "common") {

            this._subscriptionId = subscriptionId;
            this._clientId = clientId;
            this._domainName = domainName;

            // Supply a client ID if one is not already specified
            if (this._clientId.Length == 0) {
                var guid = Guid.NewGuid();
                var key = guid.ToString();
                this._clientId = key;
            }

            this._adlaAccountName = adlaAccountName;
            this._adlsAccountName = adlsAccountName;

            // Connect to Azure
            var creds = AuthenticateAzure(this._domainName, this._clientId);
            SetupClients(creds, this._subscriptionId);
        }

        public string Store(StorageFile file) {
            // Create a file if it does not already exist
            string fileName = "<Insert filepath here>"; // TODO: you must include a filepath here 
            if (!File.Exists(fileName)) {
                File.Create(fileName);
            }
            string key = Guid.NewGuid().ToString();
            UploadFile(fileName, key, true); // TODO: avoid overwrite if file already exists in DL

            // NOTE: We choose to read into a 28MB buffer because that is the default max content length for
            // an HTTP request to an Azure web server.
            int READSIZE = 28000000;
            byte[] buffer = new byte[READSIZE];
            while (file.InputStream.Position < file.InputStream.Length) {
                System.Diagnostics.Debug.WriteLine(file.InputStream.Length - file.InputStream.Position);
                System.Diagnostics.Debug.WriteLine("Before " + file.InputStream.Position);
                int diff = (int)(file.InputStream.Length - file.InputStream.Position);
                if (diff < READSIZE) {
                    buffer = new byte[diff];
                }
                file.InputStream.Read(buffer, 0, (int)Math.Min(READSIZE, diff));
                System.Diagnostics.Debug.WriteLine("After " + file.InputStream.Position);
                MemoryStream memStream = new MemoryStream(buffer);
                AppendToFile(key, memStream);
            }

            return key;
        }

        public StorageFile Retrieve(string key) {
            try {
                if (!_adlsFileSystemClient.FileSystem.PathExists(_adlsAccountName, key)) {
                    throw new KeyNotFoundException(string.Format("blob {0} does not exist", key.ToString()));
                } else {
                    Stream stream = OpenStream(key);
                    // Reasonable defaults if there's no type stored: use key as file name
                    // and octet stream as content type                   
                    return new StorageFile(stream, key, "application/octet-stream");
                }
            } catch (Exception e) when (e is ArgumentNullException || e is FormatException) {
                throw new ArgumentException(e.Message);
            }
        }

        public ServiceClientCredentials AuthenticateAzure(
        string domainName,
        string nativeClientAppCLIENTID) {
            // User login via interactive popup
            SynchronizationContext.SetSynchronizationContext(new SynchronizationContext());
            // Use the client ID of an existing AAD "Native Client" application.
            var activeDirectoryClientSettings = ActiveDirectoryClientSettings.UsePromptOnly(nativeClientAppCLIENTID, new Uri("urn:ietf:wg:oauth:2.0:oob"));
            return UserTokenProvider.LoginWithPromptAsync(domainName, activeDirectoryClientSettings).Result;
        }

        public void SetupClients(ServiceClientCredentials tokenCreds, string subscriptionId) {
            _adlaClient = new DataLakeAnalyticsAccountManagementClient(tokenCreds);
            _adlaClient.SubscriptionId = subscriptionId;

            _adlaJobClient = new DataLakeAnalyticsJobManagementClient(tokenCreds);

            _adlsFileSystemClient = new DataLakeStoreFileSystemManagementClient(tokenCreds);
        }

        public void UploadFile(string srcFilePath, string destFilePath, bool force = true) {
            var parameters = new UploadParameters(srcFilePath, destFilePath, _adlsAccountName, isOverwrite: force);
            var frontend = new DataLakeStoreFrontEndAdapter(_adlsAccountName, _adlsFileSystemClient);
            var uploader = new DataLakeStoreUploader(parameters, frontend);
            uploader.Execute();
        }

        public Stream OpenStream(string guid) {
            var stream = _adlsFileSystemClient.FileSystem.Open(_adlsAccountName, guid);
            return stream;
        }

        // Helper function to show status and wait for user input
        public void WaitForNewline(string reason, string nextAction = "") {
            Console.WriteLine(reason + "\r\nPress ENTER to continue...");

            Console.ReadLine();

            if (!String.IsNullOrWhiteSpace(nextAction))
                Console.WriteLine(nextAction);
        }

        // List all Data Lake Analytics accounts within the subscription
        public List<DataLakeAnalyticsAccount> ListADLAAccounts() {
            var response = _adlaClient.Account.List();
            var accounts = new List<DataLakeAnalyticsAccount>(response);

            while (response.NextPageLink != null) {
                response = _adlaClient.Account.ListNext(response.NextPageLink);
                accounts.AddRange(response);
            }

            Console.WriteLine("You have %i Data Lake Analytics account(s).", accounts.Count);
            for (int i = 0; i < accounts.Count; i++) {
                Console.WriteLine(accounts[i].Name);
            }

            return accounts;
        }

        public Guid SubmitJobByPath(string scriptPath, string jobName) {
            var script = File.ReadAllText(scriptPath);

            var jobId = Guid.NewGuid();
            var properties = new USqlJobProperties(script);
            var parameters = new JobInformation(jobName, JobType.USql, properties, priority: 1, degreeOfParallelism: 1, jobId: jobId);
            var jobInfo = _adlaJobClient.Job.Create(_adlaAccountName, jobId, parameters);

            return jobId;
        }

        public JobResult WaitForJob(Guid jobId) {
            var jobInfo = _adlaJobClient.Job.Get(_adlaAccountName, jobId);
            while (jobInfo.State != JobState.Ended) {
                jobInfo = _adlaJobClient.Job.Get(_adlaAccountName, jobId);
            }
            return jobInfo.Result.Value;
        }

        // Pulled from https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-get-started-net-sdk
        public void AppendToFile(string path, Stream stream) {
            _adlsFileSystemClient.FileSystem.Append(_adlsAccountName, path, stream);
        }

    }
}