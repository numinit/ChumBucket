chumbucket.StorageUi = function(document, storageClient, options) {
    this._storageClient = storageClient;
    this._bucketTable = document.querySelector(options['bucketTable']);
    this._fileTable = document.querySelector(options['fileTable']);
};

chumbucket.StorageUi.prototype.getStorageClient = function() {
    return this._storageClient;
};

chumbucket.StorageUi.prototype.getBucketTable = function() {
    return this._bucketTable;
};

chumbucket.StorageUi.prototype.getFileTable = function() {
    return this._fileTable;
};

chumbucket.StorageUi.prototype.boot = function() {
    this.updateBucketList();
};

chumbucket.StorageUi.prototype.onFailure = function(result) {
    console.log(result);
};

chumbucket.StorageUi.prototype.updateFileListForBucket = function(uri) {
    this._storageClient.listFilesInBucket(uri).then(
        chumbucket.StorageUi.prototype.onUpdateFileListSuccess.bind(this),
        chumbucket.StorageUi.prototype.onFailure.bind(this)
    );
};

chumbucket.StorageUi.prototype.updateBucketList = function(removeAllFiles) {
    var ref = this;
    var onSuccess = function(result) {
        var json = result.getResult();
        var bucketList = json['uris'].map(chumbucket.EntityUri.parse);
        var storageContentTable = ref.getBucketTable();
        var rows = storageContentTable.rows;

        // Remove all rows but the header
        for (var rowIndex = rows.length - 1; rowIndex > 0; rowIndex--) {
            storageContentTable.deleteRow(rowIndex);
        }

        // Add buckets
        bucketList.forEach(function(uri) {
            var row = storageContentTable.insertRow(-1);
            var cell = row.insertCell(0);
            var spans = [document.createElement('span'), document.createElement('span')];
            var reference = uri.toUsqlReference(), referenceTip = 'Use `' + reference + '` to reference all files in this dataset from U-SQL.';
            cell.style.textAlign = 'left';
            row.title = cell.title = spans[0].title = spans[1].title = referenceTip;
            spans[0].textContent = uri.getLeaf();
            spans[1].textContent = ' // ' + reference;
            spans[1].classList.add('reference');

            cell.appendChild(spans[0]);
            cell.appendChild(spans[1]);

            row.addEventListener('click', function() {
                ref.updateFileListForBucket(uri);
            });
        });

        // Remove all files if we need to
        if (removeAllFiles) {
            ref.removeAllFiles();
        }
    };
    return ref.getStorageClient().listBuckets().then(onSuccess, ref.onFailure);
};

chumbucket.StorageUi.prototype.removeAllFiles = function() {
    return this.removeAllRows(this.getFileTable());
};

chumbucket.StorageUi.prototype.removeAllRows = function(table) {
    var rows = table.rows;
    for (var rowIndex = rows.length - 1; rowIndex > 0; rowIndex--) {
        table.deleteRow(rowIndex);
    }
};

chumbucket.StorageUi.prototype.onUpdateFileListSuccess = function(result) {
    var ref = this;
    var json = result.getResult();
    var fileList = json['uris'].map(chumbucket.EntityUri.parse);
    var storageContentTable = ref.getFileTable();
    var rows = storageContentTable.rows;

    // Remove all files
    ref.removeAllFiles();

    fileList.forEach(function(uri) {
        var row = storageContentTable.insertRow(-1);
        var cell = [row.insertCell(0), row.insertCell(1)];
        var a = document.createElement('a');
        var span = document.createElement('span');
        var reference = uri.toUsqlReference(), referenceTip = 'Use `' + reference + '` to reference this file from U-SQL.';
        row.title = cell[0].title = a.title = span.title = referenceTip;
        cell[1].title = 'Delete ' + uri.getLeaf();
        a.textContent = uri.getLeaf();
        a.href = '#';
        a.target = '_blank';

        span.textContent = ' // ' + reference;
        span.classList.add('reference');

        cell[0].style.textAlign = 'left';
        cell[0].appendChild(a);
        cell[0].appendChild(span);

        var setDirectUri = function(callback) {
            ref.getStorageClient().getDirectFileUri(uri).then(function(directFileResult) {
                var directFileJson = directFileResult.getResult();
                var directUri = directFileJson['uri'];
                a.href = directUri;
                a.setAttribute('data-direct-uri', directUri);

                if (typeof(callback) === 'function') {
                    callback(directUri);
                }
            }, ref.onFailure);
        };

        row.addEventListener('mouseover', function(ev) {
            // We can't change the event target in the event itself, so pick an event
            // that will be triggered first. mouseover on the table row is a good candidate,
            // because it likely indicates that the file will be downloaded, and signing
            // a SAS URI is cheap.
            if (!a.getAttribute('data-direct-uri')) {
                setDirectUri();
            }
        });

        a.addEventListener('click', function(ev) {
            var direct = a.getAttribute('data-direct-uri');
            if (direct === a.href) {
                a.removeAttribute('data-direct-uri');
            } else {
                setDirectUri(function(directUri) {
                    // Too slow, they're going to get a popup notification.
                    window.open(directUri, '_blank');
                });
                ev.preventDefault();
            }
        });

        var span = document.createElement('span');
        span.classList.add('glyphicon', 'glyphicon-remove');
        cell[1].appendChild(span);

        cell[1].addEventListener('click', function(ev) {
            ref.getStorageClient().deleteFileByUri(uri).then(function() {
                ref.updateBucketList();
                row.parentElement.removeChild(row);
            }, ref.onFailure);
        });
    });
};

