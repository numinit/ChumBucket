/* @reference chumbucket/boot.js */
/* @reference chumbucket/util.js */
/* @reference chumbucket/entity_uri.js */
/* @reference chumbucket/http_client.js */
/* @reference chumbucket/analysis_client.js */
/* @reference chumbucket/analysis_ui.js */
/* @reference chumbucket/storage_client.js */
/* @reference chumbucket/storage_ui.js */
/* @reference chumbucket/upload_ui.js */

chumbucket.extend = function() {
    var obj = arguments[0];

    if (!obj || arguments.length === 0) {
        return null;
    }

    for (var i = 1; i < arguments.length; i++) {
        var names = Object.getOwnPropertyNames(arguments[i]);
        for (var j = 0; j < names.length; j++) {
            obj[names[j]] = arguments[i][names[j]];
        }
    }

    return obj;
};