/**
 * Created by Heshan.i on 1/13/2016.
 */
//-------------ResourceService Integration------------------------------------------

var restClientHandler = require('../RestClient.js');
var config = require('config');
var validator = require('validator');
var util = require('util');
var dashboardEventHandler = require('../DashboardEventHandler');
var logger = require("dvp-common/LogHandler/CommonLogHandler.js").logger;
var deepcopy = require('deepcopy');
var ardsMonitoringService = require('./ardsMonitoringService');
var Q = require('q');


var GetAttributeGroupWithDetails = function (accessToken, attributeGroupId, callback) {
    try {
        var rUrl = util.format('http://%s', config.Services.resourceServiceHost);
        if (validator.isIP(config.Services.resourceServiceHost)) {
            rUrl = util.format('http://%s:%s', config.Services.resourceServiceHost, config.Services.resourceServicePort);
        }
        var params = util.format('/DVP/API/%s/ResourceManager/Group/%d/Attribute/Details', config.Services.resourceServiceVersion, attributeGroupId);
        restClientHandler.DoGet(rUrl, params, accessToken, function (err, res, obj) {
            logger.info('GetAttributeGroupWithDetails Result:: ', obj);
            if (res && res.statusCode == 200) {
                callback(err, res, obj);
            } else {
                callback(new Error(obj), res, obj);
            }
        });
    }catch (ex2) {
        callback(new Error(ex2), null, null);
    }
};

var GetResourceDetails = function(accessToken, resourceId, callback){
    try {
        var rUrl = util.format('http://%s', config.Services.resourceServiceHost);
        if (validator.isIP(config.Services.resourceServiceHost)) {
            rUrl = util.format('http://%s:%s', config.Services.resourceServiceHost, config.Services.resourceServicePort);
        }
        var params = util.format('/DVP/API/%s/ResourceManager/Resource/%s', config.Services.resourceServiceVersion, resourceId);
        restClientHandler.DoGet(rUrl, params, accessToken, function (err, res, obj) {
            logger.info('GetResourceDetails Result:: ', obj);
            if (res && res.statusCode == 200) {
                callback(err, res, obj);
            } else {
                callback(new Error(obj), res, obj);
            }
        });
    }catch (ex2) {
        callback(new Error(ex2), null, null);
    }
};

var GetResourceTaskDetails = function(accessToken, resourceId, callback){
    try {
        var rUrl = util.format('http://%s', config.Services.resourceServiceHost);
        if (validator.isIP(config.Services.resourceServiceHost)) {
            rUrl = util.format('http://%s:%s', config.Services.resourceServiceHost, config.Services.resourceServicePort);
        }
        var params = util.format('/DVP/API/%s/ResourceManager/Resource/%s/Tasks', config.Services.resourceServiceVersion, resourceId);
        restClientHandler.DoGet(rUrl, params, accessToken, function (err, res, obj) {
            logger.info('GetResourceTaskDetails Result:: ', obj);
            if (res && res.statusCode == 200) {
                callback(err, res, obj);
            } else {
                callback(new Error(obj), res, obj);
            }
        });
    }catch (ex2) {
        callback(new Error(ex2), null, null);
    }
};

var GetResourceAttributeDetails = function(accessToken, taskInfo, callback){
    try {
        var rUrl = util.format('http://%s', config.Services.resourceServiceHost);
        if (validator.isIP(config.Services.resourceServiceHost)) {
            rUrl = util.format('http://%s:%s', config.Services.resourceServiceHost, config.Services.resourceServicePort);
        }
        var params = util.format('/DVP/API/%s/ResourceManager/ResourceTask/%d/Attributes', config.Services.resourceServiceVersion, taskInfo.ResTaskId);
        restClientHandler.DoGet(rUrl, params, accessToken, function (err, res, obj) {
            logger.info('GetResourceAttributeDetails Result:: ', obj);
            if (res && res.statusCode == 200) {
                callback(err, res, obj, taskInfo);
            } else {
                callback(new Error(obj), res, obj, taskInfo);
            }
        });
    }catch (ex2) {
        callback(new Error(ex2), null, null);
    }
};

var AddResourceStatusChangeInfo = function(accessToken, businessUnit, resourceId, statusType, status, reason, otherData, callback){

    var splitData = accessToken.split(':');
    var param2 = deepcopy(reason);
    var dashBoardReason = deepcopy(reason);

    var pOtherData = otherData.SessionId? otherData.SessionId:"";
    var jObject = {BusinessUnit: businessUnit, StatusType:statusType, Status:status, Reason:reason, OtherData: pOtherData};

    if(status.toLowerCase() === "connected" || (status.toLowerCase() === 'completed' && reason.toLowerCase() === 'afterwork')){
        param2 = util.format('%s%s', param2, otherData.Direction);
    }

    if(reason && reason.toLowerCase() !== "endbreak" && reason.toLowerCase().indexOf('break') > -1){
        dashBoardReason = 'Break';
    }

    if(splitData.length == 2) {
        var tenant = parseInt(splitData[0]);
        var company = parseInt(splitData[1]);
        var eventTime = new Date().toISOString();
        //var pubMessage = util.format("EVENT:%s:%s:%s:%s:%s:%s:%s:%s:YYYY", splitData[0], splitData[1], statusType, status, dashBoardReason, resourceId, param2, resourceId);
        dashboardEventHandler.PublishEvent(resourceId, tenant, company, businessUnit, statusType, status, dashBoardReason, resourceId, param2, resourceId, eventTime);
    }

    if(reason) {
        if(reason.toLowerCase().indexOf('end') === -1) {
            ardsMonitoringService.SendResourceStatus(accessToken, resourceId, undefined);
        }
        else{

            if(reason.toLowerCase().indexOf('endBreak') >= 0){
                ardsMonitoringService.SendResosurceStatus(accessToken, resourceId, undefined);
            }
        }

    }else {
        ardsMonitoringService.SendResourceStatus(accessToken, resourceId, undefined);
    }

    var rUrl = util.format('http://%s',config.Services.resourceServiceHost);
    if(validator.isIP(config.Services.resourceServiceHost)) {
        rUrl = util.format('http://%s:%s', config.Services.resourceServiceHost, config.Services.resourceServicePort);
    }

    var serverUrl = util.format('%s/DVP/API/%s/ResourceManager/Resource/%s/Status', rUrl, config.Services.resourceServiceVersion,resourceId);
    restClientHandler.DoPost(serverUrl,jObject, accessToken,function(err, res, obj){
        callback(err,res,obj);
    });
};

var AddResourceStatusDurationInfo = function(accessToken, businessUnit, resourceId, statusType, status, reason, otherData, sessionId, duration, callback){
    var jObject = {BusinessUnit: businessUnit, StatusType:statusType, Status:status, Reason:reason, OtherData: otherData, SessionId: sessionId, Duration: duration};

    try {
        var serverUrl = util.format("http://%s/DVP/API/%s/ResourceManager/Resource/%s/StatusDuration", config.Services.resourceServiceHost, config.Services.resourceServiceVersion, resourceId);
        if (validator.isIP(config.Services.resourceServiceHost)) {
            serverUrl = util.format("http://%s:%s/DVP/API/%s/ResourceManager/Resource/%s/StatusDuration", config.Services.resourceServiceHost, config.Services.resourceServicePort, config.Services.resourceServiceVersion, resourceId);
        }
        restClientHandler.DoPost(serverUrl,jObject, accessToken,function(err, res1, result){
            if(err){
                callback(err, undefined);
            }else{
                if(res1 && res1.statusCode === 200) {
                    callback(undefined, JSON.parse(result));
                }else{
                    callback(new Error(result), undefined);
                }
            }
        });
    }catch(ex){
        callback(ex, undefined);
    }
};

var AddResourceTaskRejectInfo = function(accessToken, businessUnit, resourceId, task, reason, otherData, sessionId, callback){
    var jObject = {BusinessUnit: businessUnit, Task:task, Reason:reason, OtherData: otherData, SessionId: sessionId};

    try {
        var serverUrl = util.format("http://%s/DVP/API/%s/ResourceManager/Resource/%s/TaskRejectInfo", config.Services.resourceServiceHost, config.Services.resourceServiceVersion, resourceId);
        if (validator.isIP(config.Services.resourceServiceHost)) {
            serverUrl = util.format("http://%s:%s/DVP/API/%s/ResourceManager/Resource/%s/TaskRejectInfo", config.Services.resourceServiceHost, config.Services.resourceServicePort, config.Services.resourceServiceVersion, resourceId);
        }
        restClientHandler.DoPost(serverUrl,jObject, accessToken,function(err, res1, result){
            if(err){
                callback(err, undefined);
            }else{
                if(res1 && res1.statusCode === 200) {
                    callback(undefined, JSON.parse(result));
                }else{
                    callback(new Error(result), undefined);
                }
            }
        });
    }catch(ex){
        callback(ex, undefined);
    }
};

var GetAttribute = function(accessToken, attId, callback){
    try {
        var rUrl = util.format('http://%s', config.Services.resourceServiceHost);
        if (validator.isIP(config.Services.resourceServiceHost)) {
            rUrl = util.format('http://%s:%s', config.Services.resourceServiceHost, config.Services.resourceServicePort);
        }
        var params = util.format('/DVP/API/%s/ResourceManager/Attribute/%s', config.Services.resourceServiceVersion, attId);
        restClientHandler.DoGet(rUrl, params, accessToken, function (err, res, obj) {
            logger.info('GetAttribute Result:: ', obj);
            callback(err, res, obj);
        });
    }catch (ex2) {
        callback(new Error(ex2), null, null);
    }
};

var GetQueueSetting = function(accessToken, queueId){
    var deferred = Q.defer();

    try {
        var rUrl = util.format('http://%s', config.Services.resourceServiceHost);
        if (validator.isIP(config.Services.resourceServiceHost)) {
            rUrl = util.format('http://%s:%s', config.Services.resourceServiceHost, config.Services.resourceServicePort);
        }
        var params = util.format('/DVP/API/%s/ResourceManager/QueueSetting/%s', config.Services.resourceServiceVersion, queueId);
        restClientHandler.DoGet(rUrl, params, accessToken, function (err, res, obj) {
            logger.info('GetQueueSetting Result:: ', obj);

            if(err){
                deferred.resolve(undefined);
            }else{
                if(obj && obj.Result){
                    deferred.resolve(obj.Result);
                }else{
                    deferred.resolve(undefined);
                }
            }

        });
    }catch (ex2) {
        deferred.resolve(new Error(ex2), undefined, undefined);
    }

    return deferred.promise;
};

var AddQueueSetting = function(accessToken, queueName, skills, serverType, requestType, callback){
    var jObject = {QueueName :queueName,MaxWaitTime:"0",Skills:skills,PublishPosition:"false",CallAbandonedThreshold:"0",ServerType:serverType,RequestType:requestType};

    try {
        var serverUrl = util.format("http://%s/DVP/API/%s/ResourceManager/QueueSetting", config.Services.resourceServiceHost, config.Services.resourceServiceVersion);
        if (validator.isIP(config.Services.resourceServiceHost)) {
            serverUrl = util.format("http://%s:%s/DVP/API/%s/ResourceManager/QueueSetting", config.Services.resourceServiceHost, config.Services.resourceServicePort, config.Services.resourceServiceVersion);
        }
        restClientHandler.DoPost(serverUrl,jObject, accessToken,function(err, res1, result){
            if(err){
                callback(err, undefined);
            }else{
                if(res1 && res1.statusCode === 200) {
                    callback(undefined, JSON.parse(result));
                }else{
                    callback(new Error(result), undefined);
                }
            }
        });
    }catch(ex){
        callback(ex, undefined);
    }
};

module.exports.GetBusinessUnitGroupSkills =  function(businessUnits,groups,accessToken, callback){

    try {
        var jObject = {businessUnits :businessUnits,groups:groups};

        var serverUrl = util.format("http://%s/DVP/API/%s/ResourceManager/BusinessUnitGroupSkills", config.Services.resourceServiceHost, config.Services.resourceServiceVersion);
        if (validator.isIP(config.Services.resourceServiceHost)) {
            serverUrl = util.format("http://%s:%s/DVP/API/%s/ResourceManager/BusinessUnitGroupSkills", config.Services.resourceServiceHost, config.Services.resourceServicePort, config.Services.resourceServiceVersion);
        }
        restClientHandler.DoPost(serverUrl,jObject, accessToken,function(err, res1, result){
            if(err){
                callback(err, undefined);
            }else{
                if(res1 && res1.statusCode === 200) {
                    callback(undefined, JSON.parse(result));
                }else{
                    callback(new Error(result), undefined);
                }
            }
        });
    }catch (ex2) {
        callback(new Error(ex2), null, null);
    }
};

module.exports.GetAttributeGroupWithDetails = GetAttributeGroupWithDetails;
module.exports.GetResourceDetails = GetResourceDetails;
module.exports.GetResourceTaskDetails = GetResourceTaskDetails;
module.exports.GetResourceAttributeDetails = GetResourceAttributeDetails;
module.exports.AddResourceStatusChangeInfo = AddResourceStatusChangeInfo;
module.exports.GetAttribute = GetAttribute;
module.exports.AddResourceStatusDurationInfo = AddResourceStatusDurationInfo;
module.exports.AddResourceTaskRejectInfo = AddResourceTaskRejectInfo;
module.exports.GetQueueSetting = GetQueueSetting;
module.exports.AddQueueSetting = AddQueueSetting;

//-------------End ResourceService Integration--------------------------------------
