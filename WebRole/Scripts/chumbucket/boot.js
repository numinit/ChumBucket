var chumbucket = (this['chumbucket'] = this['chumbucket'] || {});

chumbucket['boot'] = function(document) {
    if (chumbucket.booted_) {
        return;
    }
    chumbucket.onBoot(document);
    chumbucket.booted_ = true;
};

chumbucket.onBoot = function(document) {
    var uploader = new chumbucket.Uploader(document, { 'rootElement': '#upload', 'timingTable': '#upload-timing' });
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

        var disableAnalysis = function() {
            analysisButton.setAttribute('disabled', 'disabled');
            analysisNameField.setAttribute('disabled', 'disabled');
            analysisNameField.value = analysisJobName + ' started...';
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

        analysisClient.submitAndPoll(analysisJobName, analysisCode, function(state, result) {
            var analysisStatus = document.querySelector('#analysis-status');
            var analysisString = state.split(/_+/).map(function(s) {
                return s[0].toUpperCase() + s.slice(1).toLowerCase()
            }).join(' ');
            analysisStatus.textContent = analysisString;

            if (state === 'FAILED') {
                analysisStatus.className = 'input-group-addon bg-danger';
                enableAnalysis();
            } else if (state === 'SUCCEEDED') {
                analysisStatus.className = 'input-group-addon bg-success';
                enableAnalysis();
            } else {
                analysisStatus.className = 'input-group-addon bg-warning';
            }
        });
    });
};

chumbucket.booted_ = false;