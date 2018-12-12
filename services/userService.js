var restClientHandler = require('../RestClient.js');
var config = require('config');
var util = require('util');
var logger = require("dvp-common/LogHandler/CommonLogHandler.js").logger;

module.exports.GetBusinessUnitAndGroups =  function(resourceId,accessToken, callback){

    try {
        var rUrl = config.Services.userService;
        var params = util.format('GetBusinessUnitAndGroups/%d', resourceId);
        restClientHandler.DoGet(rUrl, params, accessToken, function (err, res, obj) {
            logger.info('GetBusinessUnitAndGroups Result:: ', obj);
            if (res.statusCode == 200) {
                callback(err, res, obj);
            } else {
                callback(new error(obj), res, obj);
            }
        });
    }catch (ex2) {
        callback(new error(ex2), null, null);
    }
};