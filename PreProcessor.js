var redisHandler = require('./RedisHandler.js');
var reqServerHandler = require('./ReqServerHandler.js');
var util = require('util');
var EventEmitter = require('events').EventEmitter;
var sort = require('./CommonMethods.js');
var infoLogger = require('./InformationLogger.js');
var reqMetaDataHandler = require('./ReqMetaDataHandler.js');
var config = require('config');
var logger = require('dvp-common/LogHandler/CommonLogHandler.js').logger;
var resourceService = require('dvp-ardscommon/services/resourceService');

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
                //var attributeNames = [];
                for (var i in sortedAttributes) {
                    var val = sortedAttributes[i];
                    attributeInfo = AppendAttributeInfo(attributeInfo, metaObj.AttributeMeta, val);
                }


                if (attributeInfo && attributeInfo.length > 0) {

                    var requestAttributes = [];
                    attributeInfo.forEach(function (attrInfo) {
                        if (attrInfo) {
                            attrInfo.AttributeCode.forEach(function (attrCode) {
                                requestAttributes.push(attrCode);
                            });
                        }
                    });

                    var sortedRequestAttributes = sort.sortData(requestAttributes);

                    var attributeDataString = util.format('attribute_%s', sortedRequestAttributes.join(":attribute_"));
                    var queueId = util.format('Queue:%d:%d:%s:%s:%s:%s', data.Company, data.Tenant, data.ServerType, data.RequestType, attributeDataString, data.Priority);
                    var queueSettingId = util.format('Queue:%d:%d:%s:%s:%s', data.Company, data.Tenant, data.ServerType, data.RequestType, attributeDataString);


                    var date = new Date();
                    var accessToken = util.format('%d:%d', data.Tenant, data.Company);
                    var requestObj = {
                        Company: data.Company,
                        Tenant: data.Tenant,
                        BusinessUnit: (data.BusinessUnit)? data.BusinessUnit: 'default',
                        ServerType: data.ServerType,
                        RequestType: data.RequestType,
                        SessionId: data.SessionId,
                        AttributeInfo: attributeInfo,
                        RequestServerId: data.RequestServerId,
                        Priority: data.Priority,
                        ArriveTime: date.toISOString(),
                        OtherInfo: data.OtherInfo,
                        ResourceCount: data.ResourceCount,
                        ServingAlgo: metaObj.ServingAlgo,
                        HandlingAlgo: metaObj.HandlingAlgo,
                        SelectionAlgo: metaObj.SelectionAlgo,
                        RequestServerUrl: url,
                        CallbackOption: option,
                        QPositionUrl: qpUrl,
                        QPositionEnable: false,
                        QueueId: queueId,
                        ReqHandlingAlgo: metaObj.ReqHandlingAlgo,
                        ReqSelectionAlgo: metaObj.ReqSelectionAlgo,
                        LbIp: config.Host.LBIP,
                        LbPort: config.Host.LBPort
                    };


                    resourceService.GetQueueSetting(accessToken, queueSettingId).then(function (queueSetting) {

                        logger.info('Queue Setting:: %s', JSON.stringify(queueSetting));

                        var publishQueuePosition = false;
                        var queueName = undefined;
                        var addQueueSettings = false;
                        if (err) {
                            publishQueuePosition = false;
                        } else {
                            if (queueSetting) {
                                publishQueuePosition = queueSetting.PublishPosition ? queueSetting.PublishPosition : false;
                                queueName = queueSetting.QueueName ? queueSetting.QueueName : undefined;
                            } else {
                                publishQueuePosition = false;
                                addQueueSettings = true;
                            }
                        }

                        if (addQueueSettings || !queueName) {
                            // --------------Set Name for QueueId--------------------------
                            var reqSkills = [];
                            for (var k = attributeInfo.length - 1; k >= 0; k--) {
                                for (var l = attributeInfo[k].AttributeNames.length - 1; l >= 0; l--) {
                                    reqSkills.push(attributeInfo[k].AttributeNames[l]);
                                }
                            }

                            var attributeNameString = util.format('%s', reqSkills.join("-"));

                            if (addQueueSettings)
                                resourceService.AddQueueSetting(accessToken, attributeNameString, sortedRequestAttributes, data.ServerType, data.RequestType, function () {
                                });

                            queueName = attributeNameString;
                            //redisHandler.AddItemToHashNX(logKey, "QueueNameHash",queueId,attributeNameString,function(){});

                        }

                        requestObj.QPositionEnable = publishQueuePosition;
                        requestObj.QueueName = queueName;

                        infoLogger.DetailLogger.log('info', '%s PreProcessor Request Queue Id: %s', logKey, queueId);
                        infoLogger.DetailLogger.log('info', '%s Finished PreProcessor. Result: %s', logKey, requestObj);
                        callback(null, requestObj);

                    }).catch(function (err) {

                        logger.error('Get Queue Settings Failed:: %s', JSON.stringify(err));

                        infoLogger.DetailLogger.log('info', '%s PreProcessor Request Queue Id: %s', logKey, queueId);
                        infoLogger.DetailLogger.log('info', '%s Finished PreProcessor. Result: %s', logKey, requestObj);

                        var reqSkills = [];
                        for (var k = attributeInfo.length - 1; k >= 0; k--) {
                            for (var l = attributeInfo[k].AttributeNames.length - 1; l >= 0; l--) {
                                reqSkills.push(attributeInfo[k].AttributeNames[l]);
                            }
                        }

                        requestObj.QueueName = util.format('%s', reqSkills.join("-"));

                        callback(null, requestObj);
                    });
                } else {
                    callback(new Error("Invalid Attributes"), null);
                }

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
                var attName = sort.FilterByID(attMeta.AttributeDetails, 'Id', att);
                if (info.AttributeGroupName == attMeta.AttributeGroupName && info.HandlingType == attMeta.HandlingType) {
                    info.AttributeCode.push(att);
                    if (attName != null) {
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
            var tempObj = {
                AttributeGroupName: attMeta.AttributeGroupName,
                HandlingType: attMeta.HandlingType,
                AttributeCode: [att],
                WeightPrecentage: attMeta.WeightPrecentage
            };
            var attName = sort.FilterByID(attMeta.AttributeDetails, 'Id', att);
            if (attName != null) {
                tempObj.AttributeNames = [attName.Name];
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
            var tags = ["company_" + data.Company, "tenant_" + data.Tenant, "serverType_" + data.ServerType, "requestType_" + data.RequestType];
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