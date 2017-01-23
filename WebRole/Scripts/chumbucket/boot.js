var chumbucket = (this['chumbucket'] = this['chumbucket'] || {});

chumbucket['boot'] = function(document, options) {
    options = options || {};
    if (chumbucket.booted_) {
        return;
    }
    chumbucket.onBoot(document, options);
    chumbucket.booted_ = true;
};

function _updateFileListForBucket(result) {
    var fileList = result.getResult()['uris'];
    var storageContentTable = document.querySelector('#file-list');
    var rows = storageContentTable.rows;

    for (var rowIndex = rows.length - 1; rowIndex > 0; rowIndex--) {
        storageContentTable.deleteRow(rowIndex);
    }

    fileList.forEach(function (uri) {
        var row = storageContentTable.insertRow(-1);
        var cell0 = row.insertCell(0);
        cell0.style.textAlign = "left";
        chumbucket._storageClient.getDirectFileUri(uri).then(function(result) {
            var fileUri = result.getResult()['uri'];
            cell0.innerHTML = "<a href=" + fileUri + ">" + uri + "</a>";
        });
    });
};

chumbucket.updateFilesListForBucket = function (uri) {
    chumbucket._storageClient.listFilesInBucket(uri).then(_updateFileListForBucket);
};

function _updateBucketList(result) {
    var bucketList = result.getResult()['uris'];
    var storageContentTable = document.querySelector('#bucket-list');
    var rows = storageContentTable.rows;

    for (var rowIndex = rows.length - 1; rowIndex > 0; rowIndex--) {
        storageContentTable.deleteRow(rowIndex);
    }  
 
    bucketList.forEach(function (uri) {
        var row = storageContentTable.insertRow(-1);
        var cell0 = row.insertCell(0);
        cell0.innerHTML = uri;
        cell0.style.textAlign = "left";

        row.addEventListener('click', function () {
            chumbucket.updateFilesListForBucket(uri);
        });
    });
};

chumbucket.updateBucketList = function () {
    chumbucket._storageClient.listBuckets().then(_updateBucketList);
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
    var storageClient = new chumbucket.StorageClient(httpClient);
    chumbucket._storageClient = storageClient;
    chumbucket.updateBucketList();

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
        var analysisConsole = document.querySelector('#analysis-console');

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

        analysisClient.submitAndPoll(analysisJobName, analysisCode, function(state, result, uri) {
            var analysisString = state.split(/_+/g).map(function(s) {
                return s[0].toUpperCase() + s.slice(1).toLowerCase()
            }).join(' ');

            if (state === 'FAILED') {
                var error = '';
                analysisStatus.className = 'input-group-addon bg-danger';
                analysisStatus.textContent = analysisString;
                analysisConsole.textContent = result['error'];
                enableAnalysis();
            } else if (state === 'SUCCEEDED') {
                analysisStatus.className = 'input-group-addon bg-success';
                analysisStatus.innerHTML = '<a href="/analysis/result?uri=' + encodeURIComponent(uri) +
                    '" target="_blank">' + analysisString + '</a>';
                analysisConsole.textContent = "Analysis succeeded. Click the link to view the results.\n\n" +
                                              "Job duration: " + chumbucket.Util.convertSecondsToString(result['durationMs'] / 1000) + "\n" +
                                              "Data read: " + chumbucket.Util.convertBytesToString(result['dataReadBytes']) + "\n" +
                                              "Throughput: " + chumbucket.Util.convertBytesToString(result['throughputBytesPerSecond']) + "/s";
                enableAnalysis();
            } else {
                analysisStatus.className = 'input-group-addon bg-warning';
                analysisStatus.textContent = analysisString;
                analysisConsole.textContent = uri + ' is in progress...';
            }
        });
    });
};

chumbucket.booted_ = false;