chumbucket.Util = function() { };

chumbucket.Util.convertBytesToString = function(bytes) {
    if (bytes < 1024) {
        // Less than a KB
        bytes = Math.round(bytes);
        return bytes + (bytes === 1 ? " byte" : " bytes");
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
};

chumbucket.Util.convertSecondsToString = function(seconds) {
    if (seconds < 60) {
        // Less than a minute
        var roundedSeconds = Math.round(seconds * 10) / 10;
        return roundedSeconds + "s";
    } else {
        // Multiple minutes
        var minutes = Math.floor(seconds / 60);
        var leftoverSeconds = seconds % 60;
        var roundedLeftoverSeconds = chumbucket.Util.convertSecondsToString(leftoverSeconds);
        return minutes + "m " + roundedLeftoverSeconds;
    }
};

chumbucket.Util.leftPad = function(n, length, padding) {
    var str = n + '';
    while (str.length < length) {
        str = padding + str;
    }
    return str;
};

chumbucket.Util.formatTime = function(time) {
    return chumbucket.Util.formatTimeComponents(
        time.getFullYear(),
        time.getMonth() + 1,
        time.getDate(),
        time.getHours(),
        time.getMinutes(),
        time.getSeconds()
    );
};

chumbucket.Util.formatTimeUtc = function(time) {
    return chumbucket.Util.formatTimeComponents(
        time.getUTCFullYear(),
        time.getUTCMonth() + 1,
        time.getUTCDate(),
        time.getUTCHours(),
        time.getUTCMinutes(),
        time.getUTCSeconds()
    );
};

chumbucket.Util.formatTimeComponents = function(year, month, day, hour, min, sec) {
    // JS is like violence: if it doesn't work, you aren't using enough of it
    return chumbucket.Util.leftPad(year, 4, '0') + '-' +
        chumbucket.Util.leftPad(month, 2, '0') + '-' +
        chumbucket.Util.leftPad(day, 2, '0') + ' ' +
        chumbucket.Util.leftPad(hour, 2, '0') + ':' +
        chumbucket.Util.leftPad(min, 2, '0') + ':' +
        chumbucket.Util.leftPad(sec, 2, '0');
};