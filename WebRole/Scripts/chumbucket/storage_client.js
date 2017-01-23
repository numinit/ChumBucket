chumbucket.StorageClient = function (http) {
    this._http = http;
};

chumbucket.StorageClient.prototype.listBuckets = function () {
    return this._http.get('file/listBuckets');
};

chumbucket.StorageClient.prototype.listFilesInBucket = function (uri) {
    return this._http.get('file/listFilesInBucket', { 'uri': uri });
};

chumbucket.StorageClient.prototype.getDirectFileUri = function (uri) {
    return this._http.get('file/getDirectUri', { 'uri': uri });
};

chumbucket.StorageClient.prototype.deleteFileByUri = function (uri) {
    return this._http.delete('file/delete', { 'uri': uri });
};
