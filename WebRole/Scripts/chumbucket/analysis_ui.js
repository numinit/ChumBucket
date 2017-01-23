﻿chumbucket.AnalysisUi = function(document, analysisClient, options) {
    this._analysisClient = analysisClient;
    this._elements = Object.create(null);

    var arr = ['submit', 'name', 'query', 'status', 'console'];
    for (var i = 0; i < arr.length; i++) {
        this._elements[arr[i]] = document.querySelector(options[arr[i]]);
    }
};

chumbucket.AnalysisUi.prototype.getAnalysisClient = function() {
    return this._analysisClient;
}

chumbucket.AnalysisUi.prototype.getElement = function(key) {
    var ret = this._elements[key];
    if (!ret) {
        throw new Error('invalid element ' + key);
    }
    return ret;
};

chumbucket.AnalysisUi.prototype.boot = function() {
    var ref = this;
    var analysisButton = ref.getElement('submit');
    analysisButton.addEventListener('click', function(ev) {
        var analysisNameField = ref.getElement('name');
        var analysisJobName = analysisNameField.value || '';
        analysisJobName = analysisJobName.trim() || ('Job ' + new Date().getTime());
        var analysisCodeField = ref.getElement('query');
        var analysisCode = analysisCodeField.value || '';
        analysisCode = analysisCode.trim();
        var analysisStatus = ref.getElement('status');
        var analysisConsole = ref.getElement('console');

        var disableAnalysis = function() {
            analysisButton.setAttribute('disabled', 'disabled');
            analysisNameField.setAttribute('disabled', 'disabled');
            analysisNameField.value = analysisJobName + ' started...';
            analysisStatus.textContent = 'Waiting';
            analysisConsole.textContent = 'Job submitted. Hold tight...';
        };

        var enableAnalysis = function() {
            analysisButton.removeAttribute('disabled');
            analysisNameField.removeAttribute('disabled');
            analysisNameField.value = '';
        };

        if (!analysisCode) {
            return;
        } else {
            disableAnalysis();
        }

        ref.getAnalysisClient().submitAndPoll(analysisJobName, analysisCode, function(state, result, uri) {
            uri = chumbucket.EntityUri.parse(uri);
            var analysisString = state.split(/_+/g).map(function(s) {
                return s[0].toUpperCase() + s.slice(1).toLowerCase();
            }).join(' ');

            if (state === 'FAILED') {
                analysisStatus.classList.remove('success', 'warning');
                analysisStatus.classList.add('danger');
                analysisStatus.textContent = analysisString;
                analysisConsole.textContent = result['error'];
                enableAnalysis();
            } else if (state === 'SUCCEEDED') {
                var a = document.createElement('a');
                a.href = '/analysis/result?uri=' + encodeURIComponent(uri.toString());
                a.target = '_blank';
                a.textContent = analysisString;
                analysisStatus.textContent = '';
                analysisStatus.classList.remove('danger', 'warning');
                analysisStatus.classList.add('success');
                analysisStatus.appendChild(a);

                var dataRead = result['dataReadBytes'];
                var duration = result['durationMs'] / 1000;
                var throughput = result['throughputBytesPerSecond'];
                analysisConsole.textContent = "Analysis succeeded. Click the link to view the results.\n\n" +
                    "Job name: " + result['name'] + "\n" +
                    "Start time: " + result['startTime'] + "\n" +
                    "Job duration: " + chumbucket.Util.convertSecondsToString(duration) + "\n" +
                    "Data read: " + chumbucket.Util.convertBytesToString(dataRead) + "\n" +
                    "Total throughput (inc. queuing): " + chumbucket.Util.convertBytesToString(dataRead / duration) + "/s\n" +
                    "Processing throughput: " + chumbucket.Util.convertBytesToString(throughput) + "/s\n" +
                    "UUID: " + uri.getLeaf();
                enableAnalysis();
            } else {
                analysisStatus.classList.remove('success', 'danger');
                analysisStatus.classList.add('warning');
                analysisStatus.textContent = analysisString;
                analysisConsole.textContent = "Analysis is in progress. Updating periodically.\n\n" +
                    "Job name: " + result['name'] + "\n" +
                    "Start time: " + result['startTime'] + "\n" +
                    "Elapsed time: " + chumbucket.Util.convertSecondsToString(result['durationMs'] / 1000) + "\n" +
                    "UUID: " + uri.getLeaf();
            }
        });
    });
};