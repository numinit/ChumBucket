var chumbucket = (window['chumbucket'] = window['chumbucket'] || {});

chumbucket['boot'] = function(document, options) {
    options = options || {};
    if (chumbucket.booted_) {
        throw new Error('already booted');
    }
    chumbucket.onBoot_(document, options);
    chumbucket.booted_ = true;
};

chumbucket.onBoot_ = function(document, options) {
    var httpClient = new chumbucket.HTTPClient();
    var keys = Object.getOwnPropertyNames(options);

    chumbucket.registry_ = Object.create(null);
    chumbucket.getRegistry = function(key) {
        var value = chumbucket.registry_[key];
        if (!value) {
            value = chumbucket.init[key](document, options[key]);
            chumbucket.registry_[key] = value;
        }
        return value;
    };

    for (var i = 0; i < keys.length; i++) {
        var key = keys[i];
        chumbucket.getRegistry(key);
    }
};

chumbucket.init = {};

chumbucket.init['http'] = function(document, options) {
    return new chumbucket.HTTPClient();
};

chumbucket.init['upload'] = function(document, options) {
    var uploadUi = new chumbucket.UploadUi(document, options);
    uploadUi.boot();
    return uploadUi;
};

chumbucket.init['storage'] = function(document, options) {
    var httpClient = chumbucket.getRegistry('http');
    var storageClient = new chumbucket.StorageClient(httpClient);
    var storageUi = new chumbucket.StorageUi(document, storageClient, options);
    storageUi.boot();
    return storageUi;
};

chumbucket.init['analysis'] = function(document, options) {
    var httpClient = chumbucket.getRegistry('http');
    var analysisClient = new chumbucket.AnalysisClient(httpClient);
    var analysisUi = new chumbucket.AnalysisUi(document, analysisClient, options);
    analysisUi.boot();
    return analysisUi;
};

chumbucket.booted_ = false;