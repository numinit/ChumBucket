chumbucket.Util = function() { };

chumbucket.Util.convertBytesToString = function(bytes) {
    if (bytes < 1024) {
        // Less than a KB
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