var redisHandler = require('./RedisHandler.js');
var reqServerHandler = require('./ReqServerHandler.js');
var util = require('util');
var EventEmitter = require('events').EventEmitter;
var sort = require('./CommonMethods.js');
var infoLogger = require('./InformationLogger.js');
var reqMetaDataHandler = require('./ReqMetaDataHandler.js');
var config = require('config');

var execute = function (logKey, data, callback) {
    infoLogger.DetailLogger.log('info', '%s +++++++++++++++++++++++++ Start PreProcessor +++++++++++++++++++++++++', logKey);

    var srs = SetRequestServer(logKey, data);
    var key = util.format('ReqMETA:%d:%d:%s:%s', data.Company, data.Tenant, data.ServerType, data.RequestType);
    var date = new Date();

    srs.on('server', function (url, option) {
        redisHandler.GetObj(logKey, key, function (err, result) {
            if (err) {
                result = reqMetaDataHandler.ReloadMetaData(data.Company, data.Tenant, data.ServerType, data.RequestType);
            }
            else if (result == null) {
                result = reqMetaDataHandler.ReloadMetaData(data.Company, data.Tenant, data.ServerType, data.RequestType);
            }
            if (result == null) {
                callback(null, null);
            } else {
                var metaObj = JSON.parse(result);

                var attributeInfo = [];
                var sortedAttributes = sort.sortData(data.Attributes);
                for (var i in sortedAttributes) {
                    var val = sortedAttributes[i];
                    attributeInfo = AppendAttributeInfo(attributeInfo, metaObj.AttributeMeta, val);
                }

                var attributeDataString = util.format('attribute_%s', sortedAttributes.join(":attribute_"));
                var queueId = util.format('Queue:%d:%d:%s:%s:%s:%s', data.Company, data.Tenant, data.ServerType, data.RequestType, attributeDataString, data.Priority.toUpperCase());
                var date = new Date();
                var requestObj = { Company: data.Company, Tenant: data.Tenant, ServerType: data.ServerType, RequestType: data.RequestType, SessionId: data.SessionId, AttributeInfo: attributeInfo, RequestServerId: data.RequestServerId, Priority: data.Priority.toUpperCase(), ArriveTime: date.toISOString(), OtherInfo: data.OtherInfo, ResourceCount: data.ResourceCount, ServingAlgo: metaObj.ServingAlgo, HandlingAlgo: metaObj.HandlingAlgo, SelectionAlgo: metaObj.SelectionAlgo, RequestServerUrl: url, CallbackOption: option, QueueId: queueId, ReqHandlingAlgo: metaObj.ReqHandlingAlgo, ReqSelectionAlgo: metaObj.ReqSelectionAlgo, LbIp: config.Host.LBIP, LbPort:config.Host.LBPort};
                infoLogger.DetailLogger.log('info', '%s PreProcessor Request Queue Id: %s', logKey, queueId);
                infoLogger.DetailLogger.log('info', '%s Finished PreProcessor. Result: %s', logKey, requestObj);
                callback(null, requestObj);
            }
        });
    });
};

var AppendAttributeInfo = function (attInfo, attMetaData, att) {
    for (var i in attInfo) {
        var info = attInfo[i];
        for (var j in attMetaData) {
            var attMeta = attMetaData[j];
            if (attMeta.AttributeCode.indexOf(att) >= 0) {
                if (info.AttributeGroupName == attMeta.AttributeGroupName && info.HandlingType == attMeta.HandlingType) {
                    info.AttributeCode.push(att);
                    return attInfo;
                }
                else {
                    var tempObj = { AttributeGroupName: attMeta.AttributeGroupName, HandlingType: attMeta.HandlingType, AttributeCode: [att], WeightPrecentage: attMeta.WeightPrecentage };
                    attInfo.push(tempObj);
                    return attInfo;
                }
            }
        }
        return attInfo;
    }

    for (var j in attMetaData) {
        var attMeta = attMetaData[j];
        if (attMeta.AttributeCode.indexOf(att) >= 0) {
            var tempObj = { AttributeGroupName: attMeta.AttributeGroupName, HandlingType: attMeta.HandlingType, AttributeCode: [att], WeightPrecentage: attMeta.WeightPrecentage };
            attInfo.push(tempObj);
            return attInfo;
        }
        else {
            return attInfo;
        }
    }


};

var SetRequestServer = function (logKey, data) {
    var e = new EventEmitter();
    process.nextTick(function () {
        if (data.RequestServerId == "0") {
            var tags = ["company_"+data.Company, "tenant_" + data.Tenant, "serverType_" + data.ServerType, "requestType_" + data.RequestType];
            reqServerHandler.SearchReqServerByTags(logKey, tags, function (err, result) {
                if (err) {
                    e.emit('server', "", "");
                }
                else {
                    var randServer = result[Math.floor(Math.random() * result.length)];
                    e.emit('server', randServer.CallbackUrl, randServer.CallbackOption);
                }
            });
        }
        else {
            reqServerHandler.GetRequestServer(logKey, data.Company, data.Tenant, data.RequestServerId, function (err, reqServerResult) {
                if (err) {
                    e.emit('server', "", "");
                }
                else {
                    var reqServerObj = JSON.parse(reqServerResult);
                    e.emit('server', reqServerObj.CallbackUrl, reqServerObj.CallbackOption);
                }
            });
        }
    });

    return (e);
};

module.exports.execute = execute;