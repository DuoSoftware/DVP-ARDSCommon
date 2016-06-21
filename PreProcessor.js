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

    srs.on('server', function (reqServerObj) {
        var url = reqServerObj.CallbackUrl;
        var option = reqServerObj.CallbackOption;
        var qpUrl = reqServerObj.QueuePositionCallbackUrl;
        var qpOption = reqServerObj.QueuePositionCallbackOption;
        var qpEnable = reqServerObj.ReceiveQueuePosition;

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
                var attributeNames = [];
                for (var i in sortedAttributes) {
                    var val = sortedAttributes[i];
                    attributeInfo = AppendAttributeInfo(attributeInfo, metaObj.AttributeMeta, val);
                }
                var reqSkills = [];
                for (var k=request.AttributeInfo.length-1; k>=0; k--) {
                    for (var l=request.AttributeInfo[k].AttributeNames.length-1; l>=0; l--) {
                        reqSkills.push(request.AttributeInfo[k].AttributeNames[l]);
                    }
                }

                var attributeDataString = util.format('attribute_%s', sortedAttributes.join(":attribute_"));
                var attributeNameString = util.format('%s', reqSkills.join("-"));
                var queueId = util.format('Queue:%d:%d:%s:%s:%s:%s', data.Company, data.Tenant, data.ServerType, data.RequestType, attributeDataString, data.Priority);
                redisHandler.AddItemToHashNX(logKey, "QueueNameHash",queueId,attributeNameString,function(){});
                var date = new Date();
                var requestObj = { Company: data.Company, Tenant: data.Tenant, ServerType: data.ServerType, RequestType: data.RequestType, SessionId: data.SessionId, AttributeInfo: attributeInfo, RequestServerId: data.RequestServerId, Priority: data.Priority, ArriveTime: date.toISOString(), OtherInfo: data.OtherInfo, ResourceCount: data.ResourceCount, ServingAlgo: metaObj.ServingAlgo, HandlingAlgo: metaObj.HandlingAlgo, SelectionAlgo: metaObj.SelectionAlgo, RequestServerUrl: url, CallbackOption: option, QPositionUrl: qpUrl, QPositionEnable: qpEnable, QueueId: queueId, ReqHandlingAlgo: metaObj.ReqHandlingAlgo, ReqSelectionAlgo: metaObj.ReqSelectionAlgo, LbIp: config.Host.LBIP, LbPort:config.Host.LBPort};
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
                var attName = sort.FilterByID(attMeta.AttributeDetails,'Id', att);
                if (info.AttributeGroupName == attMeta.AttributeGroupName && info.HandlingType == attMeta.HandlingType) {
                    info.AttributeCode.push(att);
                    if(attName != null){
                        info.AttributeNames.push(attName.Name);
                    }
                    return attInfo;
                }
                //else {
                //    var tempObj = { AttributeGroupName: attMeta.AttributeGroupName, HandlingType: attMeta.HandlingType, AttributeCode: [att], WeightPrecentage: attMeta.WeightPrecentage };
                //    if(attName != null){
                //        tempObj.AttributeNames=  [attName.Name];
                //    }
                //    attInfo.push(tempObj);
                //    return attInfo;
                //}
            }
        }
        //if(attInfo.length == i) {
        //    return attInfo;
        //}
    }

    for (var j in attMetaData) {
        var attMeta = attMetaData[j];
        if (attMeta.AttributeCode.indexOf(att) >= 0) {
            var tempObj = { AttributeGroupName: attMeta.AttributeGroupName, HandlingType: attMeta.HandlingType, AttributeCode: [att], WeightPrecentage: attMeta.WeightPrecentage };
            var attName = sort.FilterByID(attMeta.AttributeDetails,'Id', att);
            if(attName != null){
                tempObj.AttributeNames=  [attName.Name];
            }
            attInfo.push(tempObj);
            return attInfo;
        }
    }
    return attInfo;
};

var SetRequestServer = function (logKey, data) {
    var e = new EventEmitter();
    process.nextTick(function () {
        if (data.RequestServerId == "0") {
            var tags = ["company_"+data.Company, "tenant_" + data.Tenant, "serverType_" + data.ServerType, "requestType_" + data.RequestType];
            reqServerHandler.SearchReqServerByTags(logKey, tags, function (err, result) {
                if (err) {
                    e.emit('server', "");
                }
                else {
                    var randServer = result[Math.floor(Math.random() * result.length)];
                    e.emit('server', randServer);
                }
            });
        }
        else {
            reqServerHandler.GetRequestServer(logKey, data.Company, data.Tenant, data.RequestServerId, function (err, reqServerResult) {
                if (err) {
                    e.emit('server', "");
                }
                else {
                    var reqServerObj = JSON.parse(reqServerResult);
                    e.emit('server', reqServerObj);
                }
            });
        }
    });

    return (e);
};

module.exports.execute = execute;