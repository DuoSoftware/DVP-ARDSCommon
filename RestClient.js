var assert = require("assert");
var restify = require("restify");
var axios = require("axios");
var util = require("util");
var config = require("config");
var logger = require("dvp-common/LogHandler/CommonLogHandler.js").logger;

var client = function (url) {
  return restify.createJsonClient({
    url: url,
    version: "~1.0",
  });
};

var DoGet = function (url, params, internalAccessToken, callback) {
  var httpUrl = params ? util.format("%s%s", url, params) : url;
  var accessToken = util.format("Bearer %s", config.Services.accessToken);
  logger.info("DoGet:: %s", httpUrl);
  try {
    axios
      .get(httpUrl, {
        headers: {
          "content-type": "application/json",
          authorization: accessToken,
          companyinfo: internalAccessToken,
        },
      })
      .then(function (response) {
        logger.info("Server returned: %j", response.data);
        callback(null, response, response.data);
      })
      .catch(function (error) {
        logger.error("upload failed:", error.message);
        callback(
          error,
          error.response,
          error.response ? error.response.data : undefined,
        );
      });
  } catch (ex) {
    callback(ex, undefined, undefined);
  }
};

var DoPost = function (serviceurl, postData, internalAccessToken, callback) {
  var accessToken = util.format("Bearer %s", config.Services.accessToken);
  axios
    .post(serviceurl, postData, {
      headers: {
        "content-type": "application/json",
        authorization: accessToken,
        companyinfo: internalAccessToken,
      },
    })
    .then(function (response) {
      logger.info("Server returned: %j", response.data);
      callback(null, response, response.data);
    })
    .catch(function (error) {
      logger.error("upload failed:", error.message);
      callback(
        error,
        error.response,
        error.response ? error.response.data : undefined,
      );
    });
};

var DoGetSync = function (url, params) {
  client(url).get(params, function (err, req, res, obj) {
    assert.ifError(err);
    logger.info("Server returned: %j", obj);
    return obj;
  });
};

var DoPostSync = function (url, postData) {
  client(url).post(postData, function (err, req, res, obj) {
    assert.ifError(err);
    logger.info("Server returned: %j", obj);
    return obj;
  });
};

var DoPostDirect = function (serviceurl, postData, callback) {
  axios
    .post(serviceurl, postData, {
      headers: {
        "content-type": "application/json",
      },
    })
    .then(function (response) {
      logger.info("Server returned: %j", response.data);
      callback(null, response, response.data);
    })
    .catch(function (error) {
      logger.error("upload failed:", error.message);
      callback(
        error,
        error.response,
        error.response ? error.response.data : undefined,
      );
    });
};

var DoGetDirect = function (serviceurl, postData, callback) {
  var jsonStr = JSON.stringify(postData);
  var httpUrl = util.format("%s? %s", serviceurl, jsonStr);
  logger.info("RouteRequest:: %s", httpUrl);
  axios
    .get(httpUrl, {
      headers: {
        "content-type": "text/plain",
      },
    })
    .then(function (response) {
      logger.info("Server returned: %j", response.data);
      callback(null, response, response.data);
    })
    .catch(function (error) {
      logger.error("upload failed:", error.message);
      callback(
        error,
        error.response,
        error.response ? error.response.data : undefined,
      );
    });
};

var PickResource = function (url, params, callback) {
  var httpUrl = util.format("%s%s", url, params);
  var accessToken = util.format("Bearer %s", config.Services.accessToken);
  logger.info("DoGet:: %s", httpUrl);
  try {
    axios
      .get(httpUrl)
      .then(function (response) {
        logger.info("Server returned: %s", response.data);
        callback(null, response, response.data);
      })
      .catch(function (error) {
        logger.error("upload failed:", error.message);
        callback(
          error,
          error.response,
          error.response ? error.response.data : undefined,
        );
      });
  } catch (ex) {
    callback(ex, undefined, undefined);
  }
};

module.exports.DoGet = DoGet;
module.exports.DoPost = DoPost;
module.exports.DoGetSync = DoGetSync;
module.exports.DoPostSync = DoPostSync;
module.exports.DoPostDirect = DoPostDirect;
module.exports.DoGetDirect = DoGetDirect;
module.exports.PickResource = PickResource;
