var chumbucket = (this['chumbucket'] = this['chumbucket'] || {});

chumbucket['boot'] = function(document, options) {
    options = options || {};
    if (chumbucket.booted_) {
        return;
    }
    chumbucket.onBoot(document, options);
    chumbucket.booted_ = true;
};

chumbucket.onBoot = function(document, options) {
    var uploader = new chumbucket.Uploader(document, {
        'uploadEndpoint': options['uploadEndpoint'],
        'rootElement': '#upload',
        'timingTable': '#upload-timing'
    });
    var uploadButton = document.querySelector('#upload-submit');
    uploadButton.addEventListener('click', function(ev) {
        var bucketName = document.querySelector('#bucket-name').value || '';
        bucketName = bucketName.trim() || 'default';
        uploader.startUpload(bucketName);
    });

    var httpClient = new chumbucket.HTTPClient();
    var analysisClient = new chumbucket.AnalysisClient(httpClient);
    var analysisButton = document.querySelector('#analysis-submit');
    analysisButton.addEventListener('click', function(ev) {
        var analysisNameField = document.querySelector('#analysis-name');
        var analysisJobName = analysisNameField.value || '';
        analysisJobName = analysisJobName.trim() || ('Job ' + new Date().getTime());
        var analysisCodeField = document.querySelector('#analysis-query');
        var analysisCode = analysisCodeField.value || '';
        analysisCode = analysisCode.trim();
        var analysisStatus = document.querySelector('#analysis-status');

        var disableAnalysis = function() {
            analysisButton.setAttribute('disabled', 'disabled');
            analysisNameField.setAttribute('disabled', 'disabled');
            analysisNameField.value = analysisJobName + ' started...';
            analysisStatus.textContent = 'Waiting';
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

        analysisClient.submitAndPoll(analysisJobName, analysisCode, function(state, result, uri) {
            var analysisString = state.split(/_+/g).map(function(s) {
                return s[0].toUpperCase() + s.slice(1).toLowerCase()
            }).join(' ');

            if (state === 'FAILED') {
                analysisStatus.className = 'input-group-addon bg-danger';
                analysisCodeField.value = result['errorMessages'][0];
                analysisStatus.textContent = analysisString;
                enableAnalysis();
            } else if (state === 'SUCCEEDED') {
                analysisStatus.className = 'input-group-addon bg-success';
                analysisStatus.innerHTML = '<a href="/analysis/result?uri=' + encodeURIComponent(uri) +
                    '" target="_blank">' + analysisString + '</a>';
                enableAnalysis();
            } else {
                analysisStatus.className = 'input-group-addon bg-warning';
                analysisStatus.textContent = analysisString;
            }
        });
    });
};

chumbucket.booted_ = false;