chumbucket.StorageClient = function (http) {
    this._http = http;
};

chumbucket.StorageClient.prototype.listBuckets = function () {
    return this._http.get('file/listBuckets');
};

chumbucket.StorageClient.prototype.listFilesInBucket = function (uri) {
    return this._http.get('file/listFilesInBucket', { 'uri': uri });
};
