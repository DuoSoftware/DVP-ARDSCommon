/**
 * Created by Waruna on 6/1/2017.
 */

var request = require('request');
var config = require('config');
var validator = require('validator');
var util = require('util');
var logger = require('dvp-common/LogHandler/CommonLogHandler.js').logger;
var format = require("stringformat");
var notificationService = require('./services/notificationService');
var redisHandler = require('./RedisHandler.js');
var Q = require('q');
var moment = require('moment');

function getBreakThresholdValue(logKey, key) {
    var deferred = Q.defer();

    redisHandler.GetObj(logKey, key, function (err, result) {
        if (err) {
            deferred.reject(err);
        }
        else {
            deferred.resolve(result);
        }
    });
    return deferred.promise;
}

function registerCronJob(company, tenant, reference, callbackData, mainServer, time, cb) {
    try {
        if ((config.Services && config.Services.cronurl && config.Services.cronport && config.Services.cronversion)) {


            var cronURL = format("http://{0}/DVP/API/{1}/Cron", config.Services.cronurl, config.Services.cronversion);
            if (validator.isIP(config.Services.cronurl))
                cronURL = format("http://{0}:{1}/DVP/API/{2}/Cron", config.Services.cronurl, config.Services.cronport, config.Services.cronversion);

            var parsedDate = new Date();
            var newDate = new Date(parsedDate.getTime() + (1000 * time));

            var notificationMsg = {
                Reference: reference,
                Description: "ARDS Notifications",
                CronePattern: newDate.toISOString(),
                CallbackURL: mainServer,
                CallbackData: JSON.stringify(callbackData)
            };


            logger.debug("Calling cron registration service URL %s", cronURL);
            request({
                method: "POST",
                url: cronURL,
                headers: {
                    authorization: "bearer " + config.Services.accessToken,
                    companyinfo: format("{0}:{1}", tenant, company)
                },
                json: notificationMsg
            }, function (_error, _response, datax) {

                try {

                    if (!_error && _response && _response.statusCode == 200 && _response.body && _response.body.IsSuccess) {

                        return cb(true, _response.body.Result);

                    } else {

                        logger.error("There is an error in  cron registration for this");
                        return cb(false, {});


                    }
                }
                catch (excep) {

                    return cb(false, {});

                }
            });
        }
    } catch (ex) {
        logger.error('registerCronJob - [%s] - ERROR Occurred', reference, ex);
    }


}

function stopCronJob(company, tenant, id, cb) {

    try {
        if ((config.Services && config.Services.cronurl && config.Services.cronport && config.Services.cronversion)) {


            var cronURL = format("http://{0}/DVP/API/{1}/Cron/Reference/{2}/Action/destroy", config.Services.cronurl, config.Services.cronversion, id);
            if (validator.isIP(config.Services.cronurl))
                cronURL = format("http://{0}:{1}/DVP/API/{2}/Cron/Reference/{3}/Action/destroy", config.Services.cronurl, config.Services.cronport, config.Services.cronversion, id);


            logger.debug("stopCronJob service URL %s", cronURL);
            request({
                method: "POST",
                url: cronURL,
                headers: {
                    authorization: "bearer " + config.Services.accessToken,
                    companyinfo: format("{0}:{1}", tenant, company)
                }
            }, function (_error, _response, datax) {

                try {

                    if (!_error && _response && _response.statusCode == 200 && _response.body && _response.body.IsSuccess) {

                        return cb(true, _response.body.Result);

                    } else {

                        logger.error("There is an error in  stopCronJob for this");
                        return cb(false, {});


                    }
                }
                catch (excep) {

                    return cb(false, {});

                }
            });
        }
    }
    catch (ex) {
        logger.error('startBreak registerCronJob - [%s] - ERROR Occurred', id, ex);
    }

}

module.exports.startBreak = function (company, tenant, userName, resourceId, breakType, logKey) {
    try {
        var mainServer = format("http://{0}/DVP/API/{1}/ARDS/Notification/{2}", config.Host.LBIP, config.Host.Version, userName);

        if (validator.isIP(config.Host.LBIP))
            mainServer = format("http://{0}:{1}/DVP/API/{2}/ARDS/Notification/{3}", config.Host.LBIP, config.Host.LBPort, config.Host.Version, userName);

        var callbackData = {
            From: "ARDS",
            Direction: "STATELESS",
            To: userName,
            ResourceId: resourceId,
            Message: "Break Time Exceeded!",
            Event: "break_exceeded",
            RoomName: "ARDS:break_exceeded"
        };

        var breakTypeKey = util.format('BreakType:%d:%d:%s', tenant, company, breakType);
        getBreakThresholdValue(logKey, breakTypeKey)
            .then(function (val) {
                var timeJobject = JSON.parse(val);
                var time = parseInt(timeJobject.MaxDurationPerDay ? timeJobject.MaxDurationPerDay : 10)*60;
                registerCronJob(company, tenant, userName, callbackData, mainServer, time, function (isSuccess) {
                    if (isSuccess) {
                        logger.info('Create Cron Job.' + userName);
                    }
                    else {
                        logger.error('failed Create Cron Job. ' + userName);
                    }
                });
            })
            .fail(function (err) {
                console.error(err);
            })
            .done();


    }
    catch (ex) {
        logger.error('startBreak registerCronJob - [%s] - ERROR Occurred', logKey, ex);
    }
};

module.exports.endBreak = function (company, tenant, userName, logKey) {
    try {

        stopCronJob(company, tenant, userName, function (isSuccess) {
            if (isSuccess) {
                logger.info('Stop Cron Job.' + userName);
            }
            else {
                logger.error('failed Stop Cron Job. ' + userName);
            }
        });
    }
    catch (ex) {
        logger.error('endBreak stopCronJob - [%s] - ERROR Occurred', logKey, ex);

    }
};

module.exports.startFreeze = function (company, tenant, userName, resourceId, time,sessionId, logKey) {
    try {
        var mainServer = format("http://{0}/DVP/API/{1}/ARDS/Notification/{2}", config.Host.LBIP, config.Host.Version, userName);

        if (validator.isIP(config.Host.LBIP))
            mainServer = format("http://{0}:{1}/DVP/API/{2}/ARDS/Notification/{3}", config.Host.LBIP, config.Host.LBPort, config.Host.Version, userName);

        var callbackData = {
            From: "ARDS",
            Direction: "STATELESS",
            To: userName,
            ResourceId: resourceId,
            Message: "Freeze Time Exceeded!",
            Event: "freeze_exceeded",
            RoomName: "ARDS:freeze_exceeded",
            SessionID:sessionId
        };

        registerCronJob(company, tenant, userName, callbackData, mainServer, time, function (isSuccess) {
            if (isSuccess) {
                logger.info('Create Cron Job.' + userName);
            }
            else {
                logger.error('failed Create Cron Job. ' + userName);
            }
        });

    }
    catch (ex) {
        logger.error('startBreak registerCronJob - [%s] - ERROR Occurred', logKey, ex);
    }
};

module.exports.endFreeze = function (company, tenant, userName, logKey) {
    try {

        stopCronJob(company, tenant, userName, function (isSuccess) {
            if (isSuccess) {
                logger.info('Stop Cron Job.' + userName);
            }
            else {
                logger.error('failed Stop Cron Job. ' + userName);
            }
        });
    }
    catch (ex) {
        logger.error('endBreak stopCronJob - [%s] - ERROR Occurred', logKey, ex);
    }
};
