chumbucket.HTTPClient = function(base) {
    base = base || '/';
    this._base = chumbucket.HTTPClient.stripTrailingSlashes(base);
};

chumbucket.HTTPClient.stripTrailingSlashes = function(str) {
    return str.replace(/\/+$/g, '');
};

chumbucket.HTTPClient.makeAbsolute = function(base, uri) {
    return [chumbucket.HTTPClient.stripTrailingSlashes(base), uri.replace(/^\/+/g, '')].join('/');
};

chumbucket.HTTPClient.appendQuery = function(uri, query) {
    uri = uri || '';
    query = query || {};

    // Build the query string
    var keys = Object.getOwnPropertyNames(query);
    if (keys.length > 0) {
        uri += '?';
        for (var i = 0; i < keys.length; i++) {
            var key = keys[i];
            var value = query[key];
            if (!Array.isArray(value)) {
                value = [value];
            }

            for (var j = 0; j < value.length; j++) {
                uri += encodeURIComponent(key);
                uri += '=';
                uri += encodeURIComponent('' + value[j]);
                if (j < value.length - 1) {
                    uri += '&';
                }
            }
        }
    }

    return uri;
};

chumbucket.HTTPClient.prototype.get = function(uri, query, type) {
    return this.requestWithQuery('GET', uri, query, type);
};

chumbucket.HTTPClient.prototype.delete = function(uri, query, type) {
    return this.requestWithQuery('DELETE', uri, query, type);
};

chumbucket.HTTPClient.prototype.requestWithQuery = function(verb, uri, query, type) {
    query = query || {};
    type = type || 'application/json';

    var xhr = new XMLHttpRequest();
    uri = chumbucket.HTTPClient.appendQuery(uri, query);
    var promise = this.initXHR(xhr, uri, verb, type);
    xhr.send();
    return promise;
};

chumbucket.HTTPClient.prototype.post = function(uri, data, type) {
    type = type || 'application/json';
    var xhr = new XMLHttpRequest();
    var promise = this.initXHR(xhr, uri, 'POST', type);
    xhr.setRequestHeader('Content-Type', type + '; charset=UTF-8');
    xhr.setRequestHeader('Accept', type);
    xhr.send(type === 'application/json' ? JSON.stringify(data) : data);
    return promise;
};

chumbucket.HTTPClient.prototype.makeAbsolute = function(uri) {
    return chumbucket.HTTPClient.makeAbsolute(this._base, uri);
};

chumbucket.HTTPClient.prototype.initXHR = function(xhr, uri, method, type) {
    var ref = this;
    return new Promise(function(resolve, reject) {
        xhr.open(method, ref.makeAbsolute(uri), true);
        xhr.setRequestHeader('X-Requested-With', 'chumbucket/0.1');
        xhr.onreadystatechange = function(ev) {
            if (xhr.readyState !== XMLHttpRequest.DONE) {
                return;
            }

            var response = new chumbucket.HTTPClient.Response(
                xhr.status,
                xhr.statusText,
                xhr.response,
                type === 'application/json'
            );
            if (response.isSuccess()) {
                resolve(response);
            } else {
                reject(response);
            }
        };
    });
};

chumbucket.HTTPClient.Response = function(status, statusText, rawResponse, json) {
    this._status = status;
    this._statusText = this._status + ' ' + statusText;

    if (status < 200 || status > 299) {
        this._error = statusText;
    }

    if (json) {
        try {
            var response = JSON.parse(rawResponse);
            if ('result' in response) {
                this._result = response['result'];
            } else if ('error' in response) {
                this._result = null;
                this._error = response['error'];
            } else {
                this._result = response;
                this._error = 'neither result nor error in response JSON';
            }
        } catch (e) {
            this._result = null;
            this._error = 'JSON parser error: ' + e.toString();
        }
    } else {
        this._result = rawResponse;
    }
};

/**
 * Enum for HTTP response statuses.
 * @enum {number}
 */
chumbucket.HTTPClient.Response.FailureType = {
    FAILURE: -1,
    SUCCESS: 0
};

chumbucket.HTTPClient.Response.prototype.isSuccess = function() {
    return !this._error;
};

chumbucket.HTTPClient.Response.prototype.isFailure = function() {
    return !this.isSuccess();
};

chumbucket.HTTPClient.Response.prototype.getStatusCode = function() {
    return this._status;
};

chumbucket.HTTPClient.Response.prototype.getStatusText = function() {
    return this._statusText;
};

chumbucket.HTTPClient.Response.prototype.getResult = function() {
    return this._result;
};

chumbucket.HTTPClient.Response.prototype.getFailureReason = function() {
    return this._error ? this._error.toString() : null;
};
