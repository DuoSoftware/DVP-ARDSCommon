/**
 * Created by Heshan.i on 1/13/2016.
 */
//-------------ResourceService Integration------------------------------------------

var restClientHandler = require('../RestClient.js');
var config = require('config');
var validator = require('validator');
var util = require('util');

var GetAttributeGroupWithDetails = function (accessToken, attributeGroupId, callback) {
    var rUrl = util.format('http://%s',config.Services.resourceServiceHost);
    if(validator.isIP(config.Services.resourceServiceHost)) {
        rUrl = util.format('http://%s:%s', config.Services.resourceServiceHost, config.Services.resourceServicePort);
    }
    var params = util.format('/DVP/API/%s/ResourceManager/Group/%d/Attribute/Details', config.Services.resourceServiceVersion, attributeGroupId);
    restClientHandler.DoGet(rUrl, params, function (err, res, obj) {
        callback(err, res, obj);
    });
};

var GetResourceDetails = function(accessToken, resourceId, callback){
    var rUrl = util.format('http://%s',config.Services.resourceServiceHost);
    if(validator.isIP(config.Services.resourceServiceHost)) {
        rUrl = util.format('http://%s:%s', config.Services.resourceServiceHost, config.Services.resourceServicePort);
    }
    var params = util.format('/DVP/API/%s/ResourceManager/Resource/%s', config.Services.resourceServiceVersion, resourceId);
    restClientHandler.DoGet(rUrl, params, function (err, res, obj) {
        callback(err, res, obj);
    });
};

var GetResourceTaskDetails = function(accessToken, resourceId, callback){
    var rUrl = util.format('http://%s',config.Services.resourceServiceHost);
    if(validator.isIP(config.Services.resourceServiceHost)) {
        rUrl = util.format('http://%s:%s', config.Services.resourceServiceHost, config.Services.resourceServicePort);
    }
    var params = util.format('/DVP/API/%s/ResourceManager/Resource/%s/Tasks', config.Services.resourceServiceVersion, resourceId);
    restClientHandler.DoGet(rUrl, params, function (err, res, obj) {
        callback(err, res, obj);
    });
};

var GetResourceAttributeDetails = function(accessToken, taskId, callback){
    var rUrl = util.format('http://%s',config.Services.resourceServiceHost);
    if(validator.isIP(config.Services.resourceServiceHost)) {
        rUrl = util.format('http://%s:%s', config.Services.resourceServiceHost, config.Services.resourceServicePort);
    }
    var params = util.format('/DVP/API/%s/ResourceManager/ResourceTask/%d/Attributes', config.Services.resourceServiceVersion, taskId);
    restClientHandler.DoGet(rUrl, params, function (err, res, obj) {
        callback(err, res, obj);
    });
};

module.exports.GetAttributeGroupWithDetails = GetAttributeGroupWithDetails;
module.exports.GetResourceDetails = GetResourceDetails;
module.exports.GetResourceTaskDetails = GetResourceTaskDetails;
module.exports.GetResourceAttributeDetails = GetResourceAttributeDetails;

//-------------End ResourceService Integration--------------------------------------