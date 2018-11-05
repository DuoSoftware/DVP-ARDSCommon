var restClientHandler = require('../RestClient.js');
var config = require('config');
var util = require('util');
var logger = require("dvp-common/LogHandler/CommonLogHandler.js").logger;
var validator = require('validator');

module.exports.GetBusinessUnitAndGroups =  function(resourceId,accessToken, callback){

    try {

        var rUrl = util.format('http://%s', config.Services.userServiceHost);
        if (validator.isIP(config.Services.userServiceHost)) {
            rUrl = util.format('http://%s:%s', config.Services.userServiceHost, config.Services.userServicePort);
        }

        var params = util.format('/DVP/API/%s/GetBusinessUnitAndGroups/%d', config.Services.userServiceVersion, resourceId);
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