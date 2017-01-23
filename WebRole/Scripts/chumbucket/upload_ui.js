chumbucket.UploadUi = function(document, options) {
    options = options || {};

    this._uploadEndpoint = options['uploadEndpoint'];
    this._rootElement = document.querySelector(options['rootElement']);
    this._submitButtonSelector = options['submitButton'];
    this._bucketNameField = document.querySelector(options['bucketNameField']);
    this._timingTable = document.querySelector(options['timingTable']);
    this._templateClass = options['templateClass'] || 'qq-template-manual-trigger';
    this._fineUploaderOptions = options['fineUploaderOptions'] || {};
};

chumbucket.UploadUi.prototype.boot = function() {
    var ref = this;

    // Start up FineUploader
    ref.bootFineUploader(this._fineUploaderOptions);

    // This has to be deferred, since FineUploader actually creates the submit button here
    this._submitButton = document.querySelector(this._submitButtonSelector);
    this._submitButton.addEventListener('click', function(ev) {
        var bucketName = ref.getBucketNameField().value || '';
        bucketName = bucketName.trim() || 'default';
        ref.startUpload(bucketName);
    });
}

chumbucket.UploadUi.prototype.bootFineUploader = function(options) {
    options = options || {};

    var ref = this;
    var startTimes = {}, endTimes = {}, deltaTimes = {}, fileNames = {}, bucketNames = {}, fileUploadedBytes = {}, fileTotalBytes = {};

    // Start the counter at 19 so that it updates the UI the first time a chunk is sent
    var uiUpdateFreq = 20, uiUpdateCounter = uiUpdateFreq;

    var updateProgress = function(fileId) {
        var uploadedBytes = fileUploadedBytes[fileId], totalBytes = fileTotalBytes[fileId];

        // Get the row for this file from the timing table
        var curRow = document.getElementById("file-" + fileId);
        var uploadTime = curRow.cells[2];
        var uploadSpeed = curRow.cells[3];
        var bytesUploaded = curRow.cells[4];

        // Record the current time for the file
        var curDate = new Date();
        var timeElapsed = (curDate.getTime() - startTimes[fileId].getTime()) / 1000;
        uploadTime.textContent = chumbucket.Util.convertSecondsToString(timeElapsed);

        // Record the average upload speed
        uploadSpeed.textContent = chumbucket.Util.convertBytesToString(uploadedBytes / timeElapsed) + '/s';

        // Record the number of bytes uploaded
        bytesUploaded.textContent = chumbucket.Util.convertBytesToString(uploadedBytes) +
            '/' +
            chumbucket.Util.convertBytesToString(totalBytes);
    };

    var cleanUp = function(fileId) {
        // Get rid of the old info
        delete startTimes[fileId];
        delete endTimes[fileId];
        delete deltaTimes[fileId];
        delete bucketNames[fileId];
        delete fileUploadedBytes[fileId];
        delete fileTotalBytes[fileId];
    };

    var config = {
        element: ref.getRootElement(),
        template: ref.getTemplateClass(),
        blobProperties: {
            name: function(fileId) {
                var fn = function(resolve, reject) {
                    resolve(ref.getCurrentBucket() + "/" + ref.getUploader().getName(fileId));
                };
                return new Promise(fn);
            }
        },
        request: {
            endpoint: ref.getUploadEndpoint(),
        },
        signature: {
            endpoint: '/file/uploadSignature'
        },
        uploadSuccess: {
            endpoint: '/file/uploadSuccess'
        },
        autoUpload: false,
        debug: false,
        validation: {
            allowedExtensions: ['csv', 'zip'],
            acceptFiles: ['text/csv', 'application/zip']
        },
        deleteFile: {
            enabled: true
        },
        chunking: {
            enabled: true,
            concurrent: {
                enabled: true
            }
        },
        callbacks: {
            onUpload: function(fileId, name) {
                // Record the start time for submission
                startTimes[fileId] = new Date();
                fileNames[fileId] = name;

                // Create a new row in the timing table
                var timingTable = ref.getTimingTable();
                var newRow = timingTable.insertRow(1);
                newRow.setAttribute("id", "file-" + fileId);
                newRow.setAttribute("data-filename", name);

                // Create five cells in the new row (filename, status, 
                // upload time, upload speed, bytes uploaded)
                var filename = newRow.insertCell(0);
                var status = newRow.insertCell(1);
                newRow.insertCell(2);
                newRow.insertCell(3);
                newRow.insertCell(4);

                // Write the filename and status to the new row
                bucketNames[fileId] = ref.getCurrentBucket();
                if (!(fileId in bucketNames)) {
                    bucketNames[fileId] = "default";
                }
                filename.textContent = bucketNames[fileId] + "/" + name;
                status.textContent = "In Progress";
            },
            onProgress: function(fileId, name, uploadedBytes, totalBytes) {
                fileUploadedBytes[fileId] = uploadedBytes;
                fileTotalBytes[fileId] = totalBytes;
                fileNames[fileId] = name;

                // Only update the UI every 20 times onProgress is called
                uiUpdateCounter++;
                if (uiUpdateCounter >= uiUpdateFreq) {
                    uiUpdateCounter = 0;
                } else {
                    return;
                }

                updateProgress(fileId);
            },
            onCancel: function(fileId) {
                // Get the row for this file from the timing table
                var curRow = document.getElementById("file-" + fileId);
                var status = curRow.cells[1];

                // Set the status for the file to cancelled
                status.textContent = "Cancelled";
                cleanUp(fileId);
            },
            onComplete: function(fileId, name, responseJson) {
                // Determine whether the upload was successful
                var success = responseJson['success'];

                // Record the end time for submission in seconds
                endTimes[fileId] = new Date();
                deltaTimes[fileId] = (endTimes[fileId].getTime() - startTimes[fileId].getTime()) / 1000;
                fileNames[fileId] = name;

                // Do a final progress update
                fileUploadedBytes[fileId] = fileTotalBytes[fileId];
                updateProgress(fileId);

                // Update the row in the table to reflect the updated status and upload time
                var curRow = document.getElementById("file-" + fileId);
                var status = curRow.cells[1];
                var uploadTime = curRow.cells[2];
                status.textContent = success ? "Complete" : "Failed";
                uploadTime.textContent = chumbucket.Util.convertSecondsToString(deltaTimes[fileId]);

                // Reset the UI update counter
                uiUpdateCounter = uiUpdateFreq - 1;

                // Update the list of buckets
                chumbucket.getRegistry('storage').updateBucketList(true);

                // Clean up
                cleanUp(fileId);
            }
        }
    };

    this._uploader = new qq.azure.FineUploader(chumbucket.extend(config, options));
    this._currentBucket = null;
};

chumbucket.UploadUi.prototype.startUpload = function(bucketName) {
    this._currentBucket = bucketName;
    this._uploader.uploadStoredFiles();
};

chumbucket.UploadUi.prototype.getUploadEndpoint = function() {
    return this._uploadEndpoint.replace(/\/+$/g, '');
};

chumbucket.UploadUi.prototype.getCurrentBucket = function() {
    return this._currentBucket;
};

chumbucket.UploadUi.prototype.getRootElement = function() {
    return this._rootElement;
};

chumbucket.UploadUi.prototype.getSubmitButton = function() {
    return this._submitButton;
};

chumbucket.UploadUi.prototype.getBucketNameField = function() {
    return this._bucketNameField;
};

chumbucket.UploadUi.prototype.getTimingTable = function() {
    return this._timingTable;
};

chumbucket.UploadUi.prototype.getTemplateClass = function() {
    return this._templateClass;
};

chumbucket.UploadUi.prototype.getUploader = function() {
    return this._uploader;
};
