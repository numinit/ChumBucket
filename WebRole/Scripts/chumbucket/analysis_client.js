chumbucket.AnalysisClient = function(http) {
    this._http = http;
};

chumbucket.AnalysisClient.prototype.submitAndPoll = function(uri, name, code, statusCallback, interval) {
    interval = interval || 5000;

    var onFailure = function(error) {
        statusCallback('HTTP_ERROR', null);
    };

    // Submit the job
    var onSubmitSuccess = function(submitResult) {
        var submitObject = submitResult.getResult();
        var uri = submitObject['uri'];
        if (!uri) {
            statusCallback('PROTOCOL_ERROR', null);
            return;
        }

        var handle = setInterval(function() {
            var onStatusSuccess = function(statusResult) {
                var statusObject = statusResult.getResult();
                var status = statusObject['status'];
                if (!status) {
                    statusCallback('PROTOCOL_ERROR', null);
                    clearInterval(handle);
                    return;
                }

                switch (status) {
                    case 'FAILED': case 'SUCCEEDED':
                        clearInterval(handle);
                        // fall-through
                    default:
                        statusCallback(status, statusObject);
                        break;
                }
            };

            var onStatusFailure = function(statusError) {
                onFailure(statusError);
                clearInterval(handle);
            };

            this.status(uri).then(onStatusSuccess, onStatusFailure);
        }, interval);
    };

    this.submit(uri, name, code).then(onSubmitSuccess, onFailure);
};

chumbucket.AnalysisClient.prototype.submit = function(uri, name, code) {
    var params = {'uri': uri, 'name': name, 'code': code};
    return this._http.post('analysis/submit', params);
};

chumbucket.AnalysisClient.prototype.status = function(uri) {
    return this._http.get('analysis/status', {'uri': uri});
};

chumbucket.AnalysisClient.prototype.results = function(uri) {
    return this._http.get('analysis/results', {'uri': uri});
};