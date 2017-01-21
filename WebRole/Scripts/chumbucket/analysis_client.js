﻿chumbucket.AnalysisClient = function(http) {
    this._http = http;
};

chumbucket.AnalysisClient.prototype.submitAndPoll = function(name, code, statusCallback, interval) {
    interval = interval || 2500;

    var client = this;
    var onFailure = function(error) {
        statusCallback('FAILED', null);
    };

    // Submit the job
    var onSubmitSuccess = function(submitResult) {
        var submitObject = submitResult.getResult();
        var uri = submitObject['uri'];
        if (!uri) {
            statusCallback('FAILED', null);
            return;
        }

        var handle = setInterval(function() {
            var onStatusSuccess = function(statusResult) {
                var statusObject = statusResult.getResult();
                var status = statusObject['status'];
                if (!status) {
                    statusCallback('FAILED', null);
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

            client.status(uri).then(onStatusSuccess, onStatusFailure);
        }, interval);
    };

    client.submit(name, code).then(onSubmitSuccess, onFailure);
};

chumbucket.AnalysisClient.prototype.submit = function(name, code) {
    var params = {'name': name, 'code': code};
    return this._http.post('analysis/submit', params);
};

chumbucket.AnalysisClient.prototype.status = function(uri) {
    return this._http.get('analysis/status', {'uri': uri});
};

chumbucket.AnalysisClient.prototype.results = function(uri) {
    return this._http.get('analysis/results', {'uri': uri});
};