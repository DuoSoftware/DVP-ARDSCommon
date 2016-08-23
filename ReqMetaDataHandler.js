var util = require('util');
var redisHandler = require('./RedisHandler.js');
var infoLogger = require('./InformationLogger.js');
var dbConn = require('dvp-dbmodels');
var EventEmitter = require('events').EventEmitter;
var resourceService = require('./services/resourceService');


var IterateData = function (data) {
    var e = new EventEmitter();
    process.nextTick(function () {
        if (Array.isArray(data)) {
            var count = 0;
            for (var i in data) {
                var val = data[i];

                e.emit('continueIterateData', val);
                count++;

                if (data.length === count) {
                    e.emit('endIterateData');
                }
            }
        }
        else {
            e.emit('endIterateData');
        }
    });

    return (e);
};

var SetAttributeJunctionInfo = function (attributeMetaObj, attributes, callback) {
    attributeMetaObj.setArdsAttributeInfo(null).then(function (result) {
        var saji = IterateData(attributes);
        saji.on('continueIterateData', function (obj) {
            dbConn.ArdsAttributeInfo.find({ where: [{ Tenant: attributeMetaObj.Tenant }, { Company: attributeMetaObj.Company }, { Attribute: obj }] }).then(function (results) {
                if (results) {
                    results.addArdsAttributeMetaData(attributeMetaObj).then(function (result) {

                    }).catch(function (resMapGroup) {

                    });
                }
            }).error(function (err) {
                //callback(err, "Failed");
            });
        });
        saji.on('endIterateData', function (obj) {
            callback("done");
        });
    }).catch(function (resMapGroup) {
        console.log(resMapGroup);
        callback("Failed");
    });

};

var SetAttributeMetaData = function (company, tenant, reqMetaId, atrributeMetaInfo, callback) {
    var sam = IterateData(atrributeMetaInfo);
    sam.on('continueIterateData', function (obj) {
        dbConn.ArdsAttributeMetaData.create(
            {
                Tenant: tenant,
                Company: company,
                AttributeClass: obj.AttributeClass,
                AttributeType: obj.AttributeType,
                AttributeCategory: obj.AttributeCategory,
                WeightPrecentage: obj.WeightPrecentage,
                RequestMetadataId: reqMetaId
            }
        ).then(function (results) {

                SetAttributeJunctionInfo(results, obj.AttributeCode, function () { });
                //callback(null, "OK");
            }).error(function (err) {
                callback(err, "Failed");
            });
    });
    sam.on('endIterateData', function (obj) {
        callback("OK");
    });
};

var UpdateAttributeMetaData = function (company, tenant, reqMetaId, atrributeMetaInfo, callback) {
    var sam = IterateData(atrributeMetaInfo);
    sam.on('continueIterateData', function (obj) {

        dbConn.ArdsAttributeMetaData.find({ where: [{ RequestMetadataId: reqMetaId }] }).then(function (results) {
            if (results) {
                results.updateAttributes({
                    Tenant: tenant,
                    Company: company,
                    AttributeClass: obj.AttributeClass,
                    AttributeType: obj.AttributeType,
                    AttributeCategory: obj.AttributeCategory,
                    WeightPrecentage: obj.WeightPrecentage
                }).then(function (results) {

                    SetAttributeJunctionInfo(results, obj.AttributeCode, function () { });
                    //callback(null, "OK");
                }).error(function (err) {
                    callback(err, "Failed");
                });
            }
        }).error(function (err) {
            callback(err, "Failed");
        });
    });
    sam.on('endIterateData', function (obj) {
        callback("OK");
    });
};

var SetAttributeGroupInfo = function (accessToken,groupIds) {
    var e = new EventEmitter();
    process.nextTick(function () {
        if (Array.isArray(groupIds)) {
            var count = 0;
            if(groupIds.length == 0){
                e.emit('endgroupInfo');
            }else {
                for (var i in groupIds) {
                    var val = groupIds[i];
                    resourceService.GetAttributeGroupWithDetails(accessToken, val, function (err, res, obj) {
                        count++;
                        if (err) {
                            console.log(err);
                        } else {
                            if (obj.IsSuccess) {
                                var data = obj.Result;
                                var attIdList = [];
                                var attDetailList = [];
                                for (var j in data.ResAttributeGroups) {
                                    var attInfo = data.ResAttributeGroups[j];
                                    attIdList.push(attInfo.AttributeId.toString());
                                    attDetailList.push({Id:attInfo.AttributeId.toString(), Name:attInfo.ResAttribute.Attribute});
                                }
                                var tmpGroupInfo = {
                                    AttributeGroupName: data.GroupName,
                                    HandlingType: data.GroupType,
                                    WeightPrecentage: data.Percentage.toString(),
                                    AttributeCode: attIdList,
                                    AttributeDetails: attDetailList
                                };
                                e.emit('groupInfo', tmpGroupInfo);
                            }
                        }
                        if (groupIds.length === count) {
                            e.emit('endgroupInfo');
                        }
                    });

                }
            }
        }
        else {
            e.emit('endgroupInfo');
        }
    });

    return (e);
};

var AddMeataData = function (logKey, metaDataObj, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start AddMeataData *************************', logKey);
    var accessToken = util.format('%d:%d',metaDataObj.Tenant,metaDataObj.Company);
    var key = util.format('ReqMETA:%d:%d:%s:%s', metaDataObj.Company, metaDataObj.Tenant, metaDataObj.ServerType, metaDataObj.RequestType);
    var tag = ["company_" + metaDataObj.Company, "tenant_" + metaDataObj.Tenant, "serverType_" + metaDataObj.ServerType, "requestType_" + metaDataObj.RequestType, "objtype_ReqMETA"];
    var tempAttributeGroupInfo = [];
    var sagi = SetAttributeGroupInfo(accessToken, metaDataObj.AttributeGroups);

    sagi.on('groupInfo', function(obj){
        tempAttributeGroupInfo.push(obj);
    });

    sagi.on('endgroupInfo', function() {
        if(tempAttributeGroupInfo.length > 0) {
            metaDataObj.AttributeMeta = tempAttributeGroupInfo;
            var obj = JSON.stringify(metaDataObj);
            redisHandler.CheckObjExists(logKey, key, function (err, result) {
                if (err) {
                    console.log(err);
                    callback(err, "Failed");
                }
                else if (result == "0") {
                    redisHandler.AddObj_T(logKey, key, obj, tag, function (err, result) {
                        if (err) {
                            callback(err, "Failed");
                        } else {
                            infoLogger.DetailLogger.log('info', '%s Finished AddMeataData- Redis. Result: %s', logKey, result);
                            dbConn.ArdsRequestMetaData.create(
                                {
                                    Tenant: metaDataObj.Tenant,
                                    Company: metaDataObj.Company,
                                    ServerType: metaDataObj.ServerType,
                                    RequestType: metaDataObj.RequestType,
                                    AttributeGroups: JSON.stringify(metaDataObj.AttributeGroups),
                                    ServingAlgo: metaDataObj.ServingAlgo,
                                    HandlingAlgo: metaDataObj.HandlingAlgo,
                                    SelectionAlgo: metaDataObj.SelectionAlgo,
                                    ReqHandlingAlgo: metaDataObj.ReqHandlingAlgo,
                                    ReqSelectionAlgo: metaDataObj.ReqSelectionAlgo,
                                    MaxReservedTime: metaDataObj.MaxReservedTime,
                                    MaxRejectCount: metaDataObj.MaxRejectCount,
                                    MaxAfterWorkTime: metaDataObj.MaxAfterWorkTime
                                }
                            ).then(function (results) {
                                    callback(null, "OK");
                                }).error(function (err) {
                                    if(err.name == "SequelizeUniqueConstraintError"){
                                        callback(null, "OK");
                                    }else {
                                        redisHandler.RemoveObj_T(logKey,key,tag,function(){});
                                        callback(err, "Failed");
                                    }
                                });
                        }
                    });
                } else {
                    callback(new Error("Metadata Already Exsist."), "OK");
                }
            });
        }else{
            callback(new Error("No Attribute Group info found."), "Failed");
        }
    });
};

var ReaddMetaData = function (metaDataObj, callback) {
    var key = util.format('ReqMETA:%d:%d:%s:%s', metaDataObj.Company, metaDataObj.Tenant, metaDataObj.ServerType, metaDataObj.RequestType);
    var tag = ["company_" + metaDataObj.Company, "tenant_" + metaDataObj.Tenant, "serverType_" + metaDataObj.ServerType, "requestType_" + metaDataObj.RequestType, "objtype_ReqMETA"];

    var obj = JSON.stringify(metaDataObj);

    redisHandler.AddObj_T(logKey, key, obj, tag, function (err, result) {
        infoLogger.DetailLogger.log('info', 'Finished ReAddMeataData- Redis. Result: %s', result);
    });
};

var SetMeataData = function (logKey, metaDataObj, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start SetMeataData *************************', logKey);

    var key = util.format('ReqMETA:%d:%d:%s:%s', metaDataObj.Company, metaDataObj.Tenant, metaDataObj.ServerType, metaDataObj.RequestType);
    var tag = ["company_" + metaDataObj.Company, "tenant_" + metaDataObj.Tenant, "serverType_" + metaDataObj.ServerType, "requestType_" + metaDataObj.RequestType, "objtype_ReqMETA"];

    var tempAttributeGroupInfo = [];
    var accessToken = util.format('%d:%d',metaDataObj.Tenant,metaDataObj.Company);
    var sagi = SetAttributeGroupInfo(accessToken, metaDataObj.AttributeGroups);

    sagi.on('groupInfo', function(obj){
        tempAttributeGroupInfo.push(obj);
    });
    sagi.on('endgroupInfo', function() {
        metaDataObj.AttributeMeta = tempAttributeGroupInfo;
        var obj = JSON.stringify(metaDataObj);

        redisHandler.SetObj_T(logKey, key, obj, tag, function (err, result) {
            infoLogger.DetailLogger.log('info', '%s Finished SetMeataData. Result: %s', logKey, result);

            dbConn.ArdsRequestMetaData.find({ where: [{ Tenant: metaDataObj.Tenant }, { Company: metaDataObj.Company }, { ServerType: metaDataObj.ServerType }, { RequestType: metaDataObj.RequestType }] }).then(function (results) {
                if (results) {
                    results.updateAttributes({
                        AttributeGroups: JSON.stringify(metaDataObj.AttributeGroups),
                        ServingAlgo: metaDataObj.ServingAlgo,
                        HandlingAlgo: metaDataObj.HandlingAlgo,
                        SelectionAlgo: metaDataObj.SelectionAlgo,
                        ReqHandlingAlgo: metaDataObj.ReqHandlingAlgo,
                        ReqSelectionAlgo: metaDataObj.ReqSelectionAlgo,
                        MaxReservedTime: metaDataObj.MaxReservedTime,
                        MaxRejectCount: metaDataObj.MaxRejectCount,
                        MaxAfterWorkTime: metaDataObj.MaxAfterWorkTime
                    }).then(function (results) {
                        callback(null, "OK");
                    }).error(function (err) {
                        callback(err, "Failed");
                    });
                }
            }).error(function (err) {
                callback(err, "Failed");
            });

            //callback(err, result);
        });
    });
};

var GetMeataData = function (logKey, company, tenant, serverType, requestType, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start GetMeataData *************************', logKey);

    var key = util.format('ReqMETA:%s:%s:%s:%s', company, tenant, serverType, requestType);
    redisHandler.GetObj(logKey, key, function (err, result) {
        infoLogger.DetailLogger.log('info', '%s Finished GetMeataData. Result: %s', logKey, result);
        callback(err, result);
    });
};

var SearchMeataDataByTags = function (logKey, tags, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start SearchMeataDataByTags *************************', logKey);

    if (Array.isArray(tags)) {
        tags.push("objtype_ReqMETA");
        redisHandler.SearchObj_T(logKey, tags, function (err, result) {
            infoLogger.DetailLogger.log('info', '%s Finished SearchMeataDataByTags. Result: %s', logKey, result);
            callback(err, result);
        });
    }
    else {
        var e = new Error();
        e.message = "tags must be a string array";
        infoLogger.DetailLogger.log('info', '%s Finished SearchMeataDataByTags. Result: tags must be a string array', logKey);
        callback(e, null);
    }
};

var RemoveMeataData = function (logKey, company, tenant, serverType, requestType, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start RemoveMeataData *************************', logKey);

    var key = util.format('ReqMETA:%s:%s:%s:%s', company, tenant, serverType, requestType);

    redisHandler.GetObj(logKey, key, function (err, obj) {
        if (err) {
            callback(err, "false");
        }
        else {
            var metaDataObj = JSON.parse(obj);
            var tag = ["company_" + metaDataObj.Company, "tenant_" + metaDataObj.Tenant, "serverType_" + metaDataObj.ServerType, "requestType_" + metaDataObj.RequestType, "objtype_ReqMETA"];

            redisHandler.RemoveObj_T(logKey, key, tag, function (err, result) {
                dbConn.ArdsRequestMetaData.find({ where: [{ Tenant: metaDataObj.Tenant }, { Company: metaDataObj.Company }, { ServerType: metaDataObj.ServerType }, { RequestType: metaDataObj.RequestType }] }).then(function (reqMeta) {
                    if (reqMeta) {
                        reqMeta.destroy({ where: [{ Tenant: metaDataObj.Tenant }, { Company: metaDataObj.Company }, { ServerType: metaDataObj.ServerType }, { RequestType: metaDataObj.RequestType }] }).then(function (results) {
                            if (results) {

                                infoLogger.DetailLogger.log('info', '%s Finished RemoveMeataData. Result: %s', logKey, result);
                                callback(err, result);
                            }
                        }).error(function (err) {
                            callback(err, "Failed");
                        });
                    }
                }).error(function (err) {
                    callback(err, "Failed");
                });

            });
        }
    });
};

var ReloadMetaData = function (company, tenant, serverType, requestType) {
    dbConn.ArdsRequestMetaData.find({
        where: [{ Tenant: tenant }, { Company: company }, { ServerType: serverType }, { RequestType: requestType }]
    }).then(function (reqMeta) {
        if (reqMeta) {
            var accessToken = util.format('%d:%d', reqMeta.Tenant,reqMeta.Company);
            var attributeGroups = JSON.parse(reqMeta.AttributeGroups);
            var tempAttributeGroupInfo = [];
            var sagi = SetAttributeGroupInfo(accessToken, attributeGroups);

            sagi.on('groupInfo', function(obj){
                tempAttributeGroupInfo.push(obj);
            });
            sagi.on('endgroupInfo', function() {
                var metaDataObj = { Company: reqMeta.Company, Tenant: reqMeta.Tenant, ServerType: reqMeta.ServerType, RequestType: reqMeta.RequestType, ServingAlgo: reqMeta.ServingAlgo, HandlingAlgo: reqMeta.HandlingAlgo, SelectionAlgo: reqMeta.SelectionAlgo, MaxReservedTime: reqMeta.MaxReservedTime, MaxRejectCount: reqMeta.MaxRejectCount, ReqHandlingAlgo: reqMeta.ReqHandlingAlgo, ReqSelectionAlgo: reqMeta.ReqSelectionAlgo, MaxAfterWorkTime: reqMeta.MaxAfterWorkTime };
                metaDataObj.AttributeMeta = tempAttributeGroupInfo;
                ReaddMetaData(metaDataObj, function () { });
                return metaDataObj;
            });
        }
    }).error(function (err) {
        return null;
    });
};


module.exports.AddMeataData = AddMeataData;
module.exports.SetMeataData = SetMeataData;
module.exports.GetMeataData = GetMeataData;
module.exports.SearchMeataDataByTags = SearchMeataDataByTags;
module.exports.RemoveMeataData = RemoveMeataData;
module.exports.ReloadMetaData = ReloadMetaData;