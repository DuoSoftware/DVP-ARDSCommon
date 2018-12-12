var util = require('util');
var redisHandler = require('./RedisHandler.js');
var restClientHandler = require('./RestClient.js');
var reqQueueHandler = require('./ReqQueueHandler.js');
var url = require("url");
var logger = require("dvp-common/LogHandler/CommonLogHandler.js").logger;

var AddRequestServer = function (logKey, reqServerObj, callback) {
    logger.info('%s ************************* Start AddRequestServer *************************', logKey);

    if(!reqServerObj.QueuePositionCallbackUrl){
        reqServerObj.QueuePositionCallbackUrl = "";
        reqServerObj.ReceiveQueuePosition = false;
    }
    if(!reqServerObj.ReceiveQueuePosition){
        reqServerObj.ReceiveQueuePosition = false;
    }

    var key = util.format('ReqServer:%s:%s:%s', "*", "*", reqServerObj.ServerID);
    var tag = ["company_*", "tenant_*", "serverType_" + reqServerObj.ServerType, "requestType_" + reqServerObj.RequestType, "objtype_ReqServer", "serverid_" + reqServerObj.ServerID];

    var obj = JSON.stringify(reqServerObj);

    redisHandler.AddObj_T(logKey, key, obj, tag, function (err, result) {
        logger.info('%s Finished AddRequestServer. Result: %s', logKey, result);
        callback(err, result);
    });
};

var SetRequestServer = function (logKey, reqServerObj, callback) {
    logger.info('%s ************************* Start SetRequestServer *************************', logKey);

    if(!reqServerObj.QueuePositionCallbackUrl){
        reqServerObj.QueuePositionCallbackUrl = "";
        reqServerObj.ReceiveQueuePosition = false;
    }
    if(!reqServerObj.ReceiveQueuePosition){
        reqServerObj.ReceiveQueuePosition = false;
    }

    var key = util.format('ReqServer:%s:%s:%s', "*", "*", reqServerObj.ServerID);
    var tag = ["company_*", "tenant_*", "serverType_" + reqServerObj.ServerType, "requestType_" + reqServerObj.RequestType, "objtype_ReqServer", "serverid_" + reqServerObj.ServerID];

    var obj = JSON.stringify(reqServerObj);

    redisHandler.SetObj_T(logKey, key, obj, tag, function (err, result) {
        logger.info('%s Finished SetRequestServer. Result: %s', logKey, result);
        callback(err, result);
    });
};

var GetRequestServer = function (logKey, company, tenant, serverId, callback) {
    logger.info('%s ************************* Start GetRequestServer *************************', logKey);

    var key = util.format('ReqServer:%s:%s:%s', "*", "*", serverId);
    redisHandler.GetObj(logKey, key, function (err, result) {
        logger.info('%s Finished GetRequestServer. Result: %s', logKey, result);
        callback(err, result);
    });
};

var SearchReqServerByTags = function (logKey, tags, callback) {
    logger.info('%s ************************* Start SearchReqServerByTags *************************', logKey);

    if (Array.isArray(tags)) {
        tags.push("objtype_ReqServer");
        redisHandler.SearchObj_T(logKey, tags, function (err, result) {
            logger.info('%s Finished SearchReqServerByTags. Result: %s', logKey, result);
            callback(err, result);
        });
    }
    else {
        var e = new Error();
        e.message = "tags must be a string array";
        logger.info('%s Finished SearchReqServerByTags. Result: tags must be a string array', logKey);
        callback(e, null);
    }
};

var RemoveRequestServer = function (logKey, company, tenant, serverId, callback) {
    logger.info('%s ************************* Start RemoveRequestServer *************************', logKey);

    var key = util.format('ReqServer:%s:%s:%s', "*", "*", serverId);

    redisHandler.GetObj(logKey, key, function (err, obj) {
        if (err) {
            callback(err, "false");
        }
        else {
            var reqServerObj = JSON.parse(obj);
            if (reqServerObj == null) {
                var res = util.format('ReqServer %s does not exists!', serverId);
                logger.info('%s Finished RemoveRequestServer. Result: %s', logKey, res);
                callback(err, res);
            }
            else {
                var tag = ["company_*", "tenant_*", "serverType_" + reqServerObj.ServerType, "requestType_" + reqServerObj.RequestType, "objtype_ReqServer", "serverid_" + reqServerObj.ServerID];

                redisHandler.RemoveObj_T(logKey, key, tag, function (err, result) {
                    logger.info('%s Finished RemoveRequestServer. Result: %s', logKey, result);
                    callback(err, result);
                });
            }
        }
    });
};

var SendCallBack = function (logKey, serverurl, callbackOption, resultToSend, callback) {
    logger.info('%s +++++++++++++++++++++++++ Start SendCallBack Server +++++++++++++++++++++++++', logKey);
    logger.info('%s SendCallBack Server Url: %s :: ResultToSend: %s', logKey, serverurl, resultToSend);

    //var surl = util.format('%s//%s', url.parse(serverurl).protocol, url.parse(serverurl).host);
    var internalAccessToken = util.format("%d:%d", resultToSend.Tenant, resultToSend.Company);
    if(callbackOption == "GET") {
        restClientHandler.DoGetDirect(serverurl, resultToSend, function (err, res, result) {
            if(result) {
                logger.info(result);
            }
            if (err) {
                logger.error('%s Finished SendCallBack. Error: %s', logKey, err);
                callback(false, "error");
            }
            else {
                if (res && res.statusCode == "503" || result.startsWith("-ERR")) {
                    logger.info('%s Finished SendCallBack. Result: %s', logKey, "readdRequired");
                    callback(true, "readdRequired");
                }
                else if (res && res.statusCode == "200") {
                    logger.info('%s Finished SendCallBack. Result: %s', logKey, "setNext");
                    callback(true, "setNext");
                }
                else {
                    logger.info('%s Finished SendCallBack. Result: %s', logKey, "error");
                    callback(false, "error");
                }
            }
        });
    }else{
        restClientHandler.DoPost(serverurl, resultToSend, internalAccessToken, function (err, res, result) {
            if (err) {
                logger.error('%s Finished SendCallBack. Error: %s', logKey, err);
                callback(false, "error");
            }
            else {
                if (res && res.statusCode == "503" || result.startsWith("-ERR")) {
                    logger.info('%s Finished SendCallBack. Result: %s', logKey, "readdRequired");
                    callback(true, "readdRequired");
                }
                else if (res && res.statusCode == "200") {
                    logger.info('%s Finished SendCallBack. Result: %s', logKey, "setNext");
                    callback(true, "setNext");
                }
                else {
                    logger.info('%s Finished SendCallBack. Result: %s', logKey, "error");
                    callback(false, "error");
                }
            }
        });
    }
};

module.exports.AddRequestServer = AddRequestServer;
module.exports.SetRequestServer = SetRequestServer;
module.exports.GetRequestServer = GetRequestServer;
module.exports.SearchReqServerByTags = SearchReqServerByTags;
module.exports.RemoveRequestServer = RemoveRequestServer;
module.exports.SendCallBack = SendCallBack;