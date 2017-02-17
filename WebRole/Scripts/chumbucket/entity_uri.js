chumbucket.EntityUri = function(uri) {
    var nsMatch = uri.scheme().match(/^chumbucket\+(.+)$/);
    if (!nsMatch) {
        throw new Error("invalid entity URI");
    }
    this._uri = uri;
    this._ns = nsMatch[1];
};

chumbucket.EntityUri.parse = function(str) {
    return new chumbucket.EntityUri(new URI(str));
};

chumbucket.EntityUri.prototype.toString = function() {
    return this._uri.toString();
};

chumbucket.EntityUri.prototype.valueOf = function() {
    return this._uri.toString();
};

chumbucket.EntityUri.prototype.getNamespace = function() {
    return this._ns;
};

chumbucket.EntityUri.prototype.getScope = function() {
    var path = this._uri.path();
    if (!path || path === '/') {
        return 'bucket';
    } else {
        return 'key';
    }
};

chumbucket.EntityUri.prototype.getLeaf = function() {
    if (this.getScope() === 'bucket') {
        return this._uri.authority();
    } else {
        return this._uri.path().replace(/^\/+/, '');
    }
};

chumbucket.EntityUri.prototype.toUsqlReference = function() {
    var content = null;
    if (this.getScope() === 'bucket') {
        content = this._uri.authority();
    } else {
        content = this._uri.authority() + '/' + this._uri.path().replace(/^\/+/, '');
    }

    return '@in["' + content + '"]';
};