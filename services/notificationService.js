/**
 * Created by Heshan.i on 10/12/2016.
 */

var axios = require("axios");
var config = require("config");
var validator = require("validator");
var util = require("util");
var logger = require("dvp-common/LogHandler/CommonLogHandler.js").logger;

module.exports.SendNotificationToRoom = function (
  company,
  tenant,
  roomName,
  eventName,
  msgData,
  logKey,
) {
  try {
    var notificationUrl = util.format(
      "http://%s/DVP/API/%s/NotificationService/Notification/initiate/%s",
      config.Services.notificationServiceHost,
      config.Services.notificationServiceVersion,
      roomName,
    );
    if (
      config.Services.dynamicPort ||
      validator.isIP(config.Services.notificationServiceHost)
    ) {
      notificationUrl = util.format(
        "http://%s:%s/DVP/API/%s/NotificationService/Notification/initiate/%s",
        config.Services.notificationServiceHost,
        config.Services.notificationServicePort,
        config.Services.notificationServiceVersion,
        roomName,
      );
    }
    var companyInfo = util.format("%d:%d", tenant, company);

    var jsonStr = JSON.stringify(msgData);
    var accessToken = "bearer " + config.Services.accessToken;

    axios
      .post(notificationUrl, jsonStr, {
        headers: {
          "content-type": "application/json",
          authorization: accessToken,
          companyinfo: companyInfo,
          eventname: eventName,
        },
      })
      .then(function (response) {
        if (response && response.status >= 200 && response.status <= 299) {
          logger.debug(
            "[DVP-ARDSLiteService.SendNotificationToRoom] - [%s] - Send Notification Success : %s",
            logKey,
            response.data,
          );
        }
      })
      .catch(function (error) {
        logger.error(
          "[DVP-ARDSLiteService.SendNotificationToRoom] - [%s] - Send Notification Fail",
          logKey,
          error.message,
        );
      });
  } catch (ex) {
    logger.error("Do Post: Error:: " + ex);
  }
};

var SendNotificationInitiate = function (
  logKey,
  eventname,
  eventuuid,
  payload,
  companyId,
  tenantId,
) {
  try {
    var nsIp = config.Services.notificationServiceHost;
    var nsPort = config.Services.notificationServicePort;
    var nsVersion = config.Services.notificationServiceVersion;
    var token = config.Services.accessToken;

    var httpUrl = util.format(
      "http://%s/DVP/API/%s/NotificationService/Notification/initiate",
      nsIp,
      nsVersion,
    );

    if (config.Services.dynamicPort || validator.isIP(nsIp)) {
      httpUrl = util.format(
        "http://%s:%d/DVP/API/%s/NotificationService/Notification/initiate",
        nsIp,
        nsPort,
        nsVersion,
      );
    }

    var jsonStr = JSON.stringify(payload);

    logger.debug(
      "[DVP-ARDSLiteService.SendNotificationByKey] - [%s] - Creating Api Url : %s",
      logKey,
      httpUrl,
    );

    axios
      .post(httpUrl, jsonStr, {
        headers: {
          authorization: "bearer " + token,
          "content-type": "application/json",
          eventname: eventname,
          eventuuid: eventuuid,
          companyinfo: tenantId + ":" + companyId,
        },
      })
      .then(function (response) {
        if (response && response.status >= 200 && response.status <= 299) {
          logger.debug(
            "[DVP-ARDSLiteService.SendNotificationByKey] - [%s] - Send Notification Success : %s",
            logKey,
            response.data,
          );
        }
      })
      .catch(function (error) {
        logger.error(
          "[DVP-ARDSLiteService.SendNotificationByKey] - [%s] - Send Notification Fail",
          logKey,
          error.message,
        );
      });
  } catch (ex) {
    logger.error(
      "[DVP-ARDSLiteService.SendNotificationByKey] - [%s] - ERROR Occurred",
      logKey,
      ex,
    );
  }
};

module.exports.SendNotificationInitiate = SendNotificationInitiate;
