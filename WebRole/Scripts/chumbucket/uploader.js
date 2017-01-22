chumbucket.Uploader = function(document, options) {
    options = options || {};

    this._uploadEndpoint = options['uploadEndpoint'];
    this._rootElement = document.querySelector(options['rootElement']);
    this._timingTable = document.querySelector(options['timingTable']);
    this._templateClass = options['templateClass'] || 'qq-template-manual-trigger';

    var startTimes = {}, endTimes = {}, deltaTimes = {}, bucketNames = {};

    var ref = this;
    var config = {
        element: ref.getRootElement(),
        template: ref.getTemplateClass(),
        blobProperties: {
            name: function(id) {
                var fn = function(resolve, reject) {
                    bucketNames[ref.getUploader().getName(id)] = ref.getCurrentBucket();
                    resolve(ref.getCurrentBucket() + "/" + ref.getUploader().getName(id));
                };
                return new Promise(fn);
            }
        },
        request: {
            endpoint: ref.getUploadEndpoint(),
        },
        signature: {
            endpoint: '/file/sas'
        },
        uploadSuccess: {
            endpoint: '/file/success',
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
                startTimes[name] = new Date();
            },
            onComplete: function (fileId, name, responseJSON) {
                // TODO: see if the upload was successful
                var success =  responseJSON['success'];

                // Record the end time for submission in seconds
                endTimes[name] = new Date();
                deltaTimes[name] = (endTimes[name].getTime() - startTimes[name].getTime()) / 1000;

                // Create a new row in the timing table
                var timingTable = ref.getTimingTable();
                var newRow = timingTable.insertRow(1);
                newRow.setAttribute('data-filename', name);

                // Write the filename, status, and upload time to the new row
                var cell1 = newRow.insertCell(0);
                var cell2 = newRow.insertCell(1);
                var cell3 = newRow.insertCell(2);
                cell1.textContent = bucketNames[name] + "/" + name;
                cell2.textContent = success ? "Complete" : "Failed";
                cell3.textContent = deltaTimes[name] + " seconds";

                delete startTimes[name];
                delete endTimes[name];
                delete deltaTimes[name];
                delete bucketNames[name];
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