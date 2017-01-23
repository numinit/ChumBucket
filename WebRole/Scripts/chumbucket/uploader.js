chumbucket.Uploader = function(document, options) {
    options = options || {};

    this._uploadEndpoint = options['uploadEndpoint'];
    this._rootElement = document.querySelector(options['rootElement']);
    this._timingTable = document.querySelector(options['timingTable']);
    this._templateClass = options['templateClass'] || 'qq-template-manual-trigger';

    var startTimes = {}, endTimes = {}, deltaTimes = {}, bucketNames = {};
    // Start the counter at 19 so that it updates the UI the first time a chunk is sent
    var uiUpdateFreq = 20;
    var uiUpdateCounter = uiUpdateFreq;

    var ref = this;
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
            endpoint: '/file/uploadSas'
        },
        uploadSuccess: {
            endpoint: '/file/uploadSuccess',
            params: {
                mimeType: 'text/csv'
            }
        },
        autoUpload: false,
        debug: true,
        validation: {
            allowedExtensions: ['csv'],
            acceptFiles: ['text/csv']
        },
        deleteFile: {
            enabled: false
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
                console.log("bucketNames:");
                console.log(bucketNames);
                if (!(fileId in bucketNames)) {
                    bucketNames[fileId] = "default";
                }
                filename.textContent = bucketNames[fileId] + "/" + name;
                status.textContent = "In Progress";
            },
            onProgress: function (fileId, name, uploadedBytes, totalBytes) {
                // Only update the UI every 20 times onProgress is called
                uiUpdateCounter++;
                if (uiUpdateCounter >= uiUpdateFreq) {
                    uiUpdateCounter = 0;
                } else {
                    return;
                }

                // Get the row for this file from the timing table
                var curRow = document.getElementById("file-" + fileId);
                var uploadTime = curRow.cells[2];
                var uploadSpeed = curRow.cells[3];
                var bytesUploaded = curRow.cells[4];

                // Record the current time for the file
                var curDate = new Date();
                var timeElapsed = (curDate.getTime() - startTimes[fileId].getTime()) / 1000;
                uploadTime.textContent = convertSecondsToString(timeElapsed);

                // Record the average upload speed
                uploadSpeed.textContent = convertBytesToString(uploadedBytes / timeElapsed) + " per second";

                // Record the number of bytes uploaded
                bytesUploaded.textContent = convertBytesToString(uploadedBytes) + "/" +
                    convertBytesToString(totalBytes);
            },
            onCancel: function(fileId) {
                // Get the row for this file from the timing table
                var curRow = document.getElementById("file-" + fileId);
                var status = curRow.cells[1];

                // Set the status for the file to cancelled
                status.textContent = "Cancelled";
            },
            onComplete: function (fileId, name, responseJson) {
                // Determine whether the upload was successful
                var success = responseJson['success'];
                console.log(responseJson);

                // Record the end time for submission in seconds
                endTimes[fileId] = new Date();
                deltaTimes[fileId] = (endTimes[fileId].getTime() - startTimes[fileId].getTime()) / 1000;

                // Update the row in the table to reflect the updated status and upload time
                var curRow = document.getElementById("file-" + fileId);
                var status = curRow.cells[1];
                var uploadTime = curRow.cells[2];
                status.textContent = success ? "Complete" : "Failed";
                uploadTime.textContent = convertSecondsToString(deltaTimes[fileId]);

                // Reset the UI update counter
                uiUpdateCounter = uiUpdateFreq - 1;

                // Get rid of the old info
                delete startTimes[fileId];
                delete endTimes[fileId];
                delete deltaTimes[fileId];
                delete bucketNames[fileId];

                // Updates the list of bucket in UI - defined in boot.js
                chumbucket.updateBucketList();
            }
        }
    };

    this._uploader = new qq.azure.FineUploader(chumbucket.extend(config, options));
    this._currentBucket = null;
};

chumbucket.Uploader.prototype.startUpload = function(bucketName) {
    this._currentBucket = bucketName;
    this._uploader.uploadStoredFiles();
};

chumbucket.Uploader.prototype.getUploadEndpoint = function() {
    return this._uploadEndpoint.replace(/\/+$/g, '');
};

chumbucket.Uploader.prototype.getCurrentBucket = function() {
    return this._currentBucket;
};

chumbucket.Uploader.prototype.getRootElement = function() {
    return this._rootElement;
};

chumbucket.Uploader.prototype.getTimingTable = function() {
    return this._timingTable;
};

chumbucket.Uploader.prototype.getTemplateClass = function() {
    return this._templateClass;
};

chumbucket.Uploader.prototype.getUploader = function() {
    return this._uploader;
};

convertBytesToString = function (bytes) {
    if (bytes < 1024) {
        // Less than a KB
        return bytes + " bytes";
    } else if (bytes < 1024 * 1024) {
        // Less than a MB
        var kilobytes = bytes / 1024;
        var roundedKilobytes = Math.round(kilobytes * 10) / 10;
        return roundedKilobytes + " kB";
    } else if (bytes < 1024 * 1024 * 1024) {
        // Less than a GB
        var megabytes = bytes / 1024 / 1024;
        var roundedMegabytes = Math.round(megabytes * 10) / 10;
        return roundedMegabytes + " MB";
    } else {
        // Multiple GB
        var gigabytes = bytes / 1024 / 1024 / 1024;
        var roundedGigabytes = Math.round(gigabytes * 10) / 10;
        return roundedGigabytes + " GB";
    }
}

convertSecondsToString = function(seconds) {
    if (seconds < 60) {
        // Less than a minute
        var roundedSeconds = Math.round(seconds * 10) / 10;
        return roundedSeconds + " s";
    } else {
        // Multiple minutes
        var minutes = Math.floor(seconds / 60);
        var leftoverSeconds = seconds % 60;
        var roundedLeftoverSeconds = convertSecondsToString(leftoverSeconds);
        return minutes + " m " + roundedLeftoverSeconds;
    }
}