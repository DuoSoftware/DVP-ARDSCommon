/**
 * Created by Heshan.i on 1/13/2016.
 */
//-------------ResourceService Integration------------------------------------------

var restClientHandler = require('../RestClient.js');
var config = require('config');
var validator = require('validator');
var util = require('util');
var redisHandler = require('../RedisHandler.js');

var GetAttributeGroupWithDetails = function (accessToken, attributeGroupId, callback) {
    var rUrl = util.format('http://%s',config.Services.resourceServiceHost);
    if(validator.isIP(config.Services.resourceServiceHost)) {
        rUrl = util.format('http://%s:%s', config.Services.resourceServiceHost, config.Services.resourceServicePort);
    }
    var params = util.format('/DVP/API/%s/ResourceManager/Group/%d/Attribute/Details', config.Services.resourceServiceVersion, attributeGroupId);
    restClientHandler.DoGet(rUrl, params, accessToken, function (err, res, obj) {
        callback(err, res, obj);
    });
};

var GetResourceDetails = function(accessToken, resourceId, callback){
    var rUrl = util.format('http://%s',config.Services.resourceServiceHost);
    if(validator.isIP(config.Services.resourceServiceHost)) {
        rUrl = util.format('http://%s:%s', config.Services.resourceServiceHost, config.Services.resourceServicePort);
    }
    var params = util.format('/DVP/API/%s/ResourceManager/Resource/%s', config.Services.resourceServiceVersion, resourceId);
    restClientHandler.DoGet(rUrl, params, accessToken, function (err, res, obj) {
        callback(err, res, obj);
    });
};

var GetResourceTaskDetails = function(accessToken, resourceId, callback){
    var rUrl = util.format('http://%s',config.Services.resourceServiceHost);
    if(validator.isIP(config.Services.resourceServiceHost)) {
        rUrl = util.format('http://%s:%s', config.Services.resourceServiceHost, config.Services.resourceServicePort);
    }
    var params = util.format('/DVP/API/%s/ResourceManager/Resource/%s/Tasks', config.Services.resourceServiceVersion, resourceId);
    restClientHandler.DoGet(rUrl, params, accessToken, function (err, res, obj) {
        callback(err, res, obj);
    });
};

var GetResourceAttributeDetails = function(accessToken, taskId, callback){
    var rUrl = util.format('http://%s',config.Services.resourceServiceHost);
    if(validator.isIP(config.Services.resourceServiceHost)) {
        rUrl = util.format('http://%s:%s', config.Services.resourceServiceHost, config.Services.resourceServicePort);
    }
    var params = util.format('/DVP/API/%s/ResourceManager/ResourceTask/%d/Attributes', config.Services.resourceServiceVersion, taskId);
    restClientHandler.DoGet(rUrl, params, accessToken, function (err, res, obj) {
        callback(err, res, obj);
    });
};

var AddResourceStatusChangeInfo = function(accessToken, resourceId, statusType, status, reason, otherData, callback){
    var jObject = {StatusType:statusType, Status:status, Reason:reason, OtherData: otherData};

    var splitData = accessToken.split(':');
    if(splitData.length == 2) {
        var pubMessage = util.format("EVENT:%s:%s:%s:%s:%s:%s:%s:%s:YYYY", splitData[0], splitData[1], statusType, status, reason, resourceId, "param2", resourceId);
        redisHandler.Publish("DashBoardEvent", "events", pubMessage, function () {
        });
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

module.exports.GetAttributeGroupWithDetails = GetAttributeGroupWithDetails;
module.exports.GetResourceDetails = GetResourceDetails;
module.exports.GetResourceTaskDetails = GetResourceTaskDetails;
module.exports.GetResourceAttributeDetails = GetResourceAttributeDetails;
module.exports.AddResourceStatusChangeInfo = AddResourceStatusChangeInfo;

//-------------End ResourceService Integration--------------------------------------
