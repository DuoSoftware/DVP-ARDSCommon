/**
 * Created by Heshan.i on 5/26/2017.
 */


var restClientHandler = require('../RestClient.js');
var config = require('config');
var validator = require('validator');
var util = require('util');
var logger = require('dvp-common/LogHandler/CommonLogHandler.js').logger;

function SendResourceStatus(accessToken, resourceId, additionalParams){

    var rUrl = util.format('http://%s',config.Services.ardsMonitoringServiceHost);
    if(validator.isIP(config.Services.ardsMonitoringServiceHost)) {
        rUrl = util.format('http://%s:%s', config.Services.ardsMonitoringServiceHost, config.Services.ardsMonitoringServicePort);
    }

    var sendStatusUrl = util.format('%s/DVP/API/%s/ARDS/MONITORING/resource/%s/status/publish', rUrl, config.Services.ardsMonitoringServiceVersion,resourceId);
    if(additionalParams){
        sendStatusUrl = util.format('%s?%s', sendStatusUrl, additionalParams);
    }

    restClientHandler.DoGet(sendStatusUrl,undefined, accessToken,function(err, res, obj){
        if(err) {
            logger.error('SendResourceStatus - %s :: Error - %s ', resourceId, err);
        }else {
            logger.info('SendResourceStatus - %s :: Message - %s ', resourceId, obj);
        }
    });

}


module.exports.SendResourceStatus = SendResourceStatus;