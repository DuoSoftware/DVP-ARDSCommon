/**
 * Created by Waruna on 6/1/2017.
 */

var request = require('request');
var config = require('config');
var validator = require('validator');
var util = require('util');
var logger = require('dvp-common/LogHandler/CommonLogHandler.js').logger;
var format = require("stringformat");
var notificationService = require('./notificationService');

function RegisterCronJob(company, tenant, userName, mainServer, cb) {

    if ((config.Services && config.Services.cronurl && config.Services.cronport && config.Services.cronversion)) {


        var cronURL = format("http://{0}/DVP/API/{1}/Cron", config.Services.cronurl, config.Services.cronversion);
        if (validator.isIP(config.Services.cronurl))
            cronURL = format("http://{0}:{1}/DVP/API/{2}/Cron", config.Services.cronurl, config.Services.cronport, config.Services.cronversion);

        var time = 10;
        var notificationMsg = {
            Reference: userName,
            Description: "Direct message twitter",
            CronePattern: format("*/{0} * * * *", time),
            CallbackURL: mainServer,
            CallbackData: {
                From: "ARDS",
                Direction: "STATELESS",
                To: userName,
                Message: "Break Time Exceeded!"
            }
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

}

function StopCronJob(company, tenant, id, cb) {

    if ((config.Services && config.Services.cronurl && config.Services.cronport && config.Services.cronversion)) {


        var cronURL = format("http://{0}/DVP/API/{1}/Cron/Reference/{2}/Action/stop", config.Services.cronurl, config.Services.cronversion, id);
        if (validator.isIP(config.Services.cronurl))
            cronURL = format("http://{0}:{1}/DVP/API/{2}/Cron/Reference/{3}/Action/stop", config.Services.cronurl, config.Services.cronport, config.Services.cronversion, id);


        logger.debug("StopCronJob service URL %s", cronURL);
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

                    logger.error("There is an error in  StopCronJob for this");
                    return cb(false, {});


                }
            }
            catch (excep) {

                return cb(false, {});

            }
        });
    }

}

module.exports.StartBreak = function (company, tenant, userName, logKey) {
    try {
        var mainServer = format("http://{0}/DVP/API/{1}/ARDS/Notification/{2}", config.LBServer.ip, config.Host.version, userName);

        if (validator.isIP(config.LBServer.ip))
            mainServer = format("http://{0}:{1}/DVP/API/{2}/ARDS/Notification/{3}", config.LBServer.ip, config.LBServer.port, config.Host.version, userName);

        RegisterCronJob(company, tenant, userName, mainServer, function (isSuccess) {
            if (isSuccess) {
                logger.error('failed Create Cron Job. ' + userName);
            }
            else {
                logger.info('Create Cron Job.' + userName);
            }
        });

    }
    catch (ex) {
        logger.error('StartBreak RegisterCronJob - [%s] - ERROR Occurred', logKey, ex);
    }
};

module.exports.EndBreak = function (company, tenant, userName,logKey) {
    try {

        StopCronJob(company, tenant, userName, function (isSuccess) {
            if (isSuccess) {
                logger.error('failed Stop Cron Job. ' + userName);
            }
            else {
                logger.info('Stop Cron Job.' + userName);
            }
        });
    }
    catch (ex) {
        logger.error('EndBreak StopCronJob - [%s] - ERROR Occurred', logKey, ex);

    }
};

module.exports.SendNotification = function (company, tenant, userName,logKey) {
    try {

        /*From: "ARDS",
            Direction: "STATELESS",
            To: userName,
            Message: "Break Time Exceeded!"*/

        var notificationMsg = {
            From: "ARDS",
            Direction:"STATELESS",
            To:userName,
            Message: "Break Time Exceeded!"
        };
        notificationService.SendNotificationInitiate(logKey, "message", "", notificationMsg, company, tenant);
    }
    catch (ex) {
        logger.error('SendNotification - [%s] - ERROR Occurred', logKey, ex);
    }
};