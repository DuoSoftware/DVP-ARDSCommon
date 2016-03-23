var util = require('util');
var redisHandler = require('./RedisHandler.js');
var EventEmitter = require('events').EventEmitter;
var sortArray = require('./CommonMethods.js');
var restClientHandler = require('./RestClient.js');
var infoLogger = require('./InformationLogger.js');
var config = require('config');
var validator = require('validator');
var resourceService = require('./services/resourceService');
var resourceStateMapper = require('./ResourceStateMapper');
var deepcopy = require("deepcopy");
var commonMethods = require('./CommonMethods');

var PreProcessTaskData = function(accessToken, taskInfos){
    var e = new EventEmitter();
    process.nextTick(function () {
        if (Array.isArray(taskInfos)) {
            var count = 0;
            for (var i in taskInfos) {
                var taskInfo = taskInfos[i];
                var task = {HandlingType:taskInfo.ResTask.ResTaskInfo.TaskType, NoOfSlots:taskInfo.Concurrency, RefInfo:taskInfo.RefInfo};
                var attributes = [];
                resourceService.GetResourceAttributeDetails(accessToken,taskInfo.ResTaskId, function(resAttErr, resAttRes, resAttObj){
                    if(resAttErr) {
                        count++;
                        consile.log(resAttErr);
                        e.emit('taskInfo', task, attributes);
                        if (taskInfos.length === count) {
                            e.emit('endTaskInfo');
                        }
                    }else{
                        var ppad = PreProcessAttributeData(task.HandlingType,resAttObj.Result.ResResourceAttributeTask);
                        ppad.on('attributeInfo', function(attribute){
                            attributes.push(attribute);
                        });
                        ppad.on('endAttributeInfo', function(){
                            count++;
                            e.emit('taskInfo', task, attributes);
                            if (taskInfos.length === count) {
                                e.emit('endTaskInfo');
                            }
                        });
                    }
                });
            }
        }
        else {
            e.emit('endTaskInfo');
        }
    });

    return (e);
};

var PreProcessAttributeData = function(handlingType, attributeInfos){
    var e = new EventEmitter();
    process.nextTick(function () {
        if (Array.isArray(attributeInfos)) {
            var count = 0;
            for (var i in attributeInfos) {
                var attributeInfo = attributeInfos[i];
                count++;
                var attribute = {Attribute:attributeInfo.AttributeId.toString(), HandlingType:handlingType, Percentage:attributeInfo.Percentage};
                e.emit('attributeInfo', attribute);

                if (attributeInfos.length === count) {
                    e.emit('endAttributeInfo');
                }
            }
        }
        else {
            e.emit('endAttributeInfo');
        }
    });

    return (e);
};

var PreProcessResourceData = function(logKey, accessToken, preResourceData, callback){
    resourceService.GetResourceTaskDetails(accessToken,preResourceData.ResourceId,function(taskErr, taskRes, taskObj){
        var newAttributeInfo = [];
        if(taskErr){
            callback(taskErr, taskRes, preResourceData,newAttributeInfo);
        }else{
            if(taskObj.IsSuccess){
                var pptd = PreProcessTaskData(accessToken, taskObj.Result);
                pptd.on('taskInfo', function(taskInfo, attributeInfo){
                    preResourceData.ConcurrencyInfo.push(taskInfo);
                    for(var i in attributeInfo){
                        var attrInfo = attributeInfo[i];
                        preResourceData.ResourceAttributeInfo.push(attrInfo);
                        newAttributeInfo.push(attrInfo);
                    }
                });
                pptd.on('endTaskInfo', function(){
                    preResourceData.ResourceAttributeInfo = commonMethods.UniqueObjectArray(preResourceData.ResourceAttributeInfo, 'Attribute');;
                    callback(null, "", preResourceData, newAttributeInfo);
                });
            }else{
                callback(taskObj.Exception, taskObj.CustomMessage, preResourceData,newAttributeInfo);
            }
        }
    });
};

var SetConcurrencyInfo = function (data) {
    var e = new EventEmitter();
    process.nextTick(function () {
        if (Array.isArray(data)) {
            var count = 0;
            for (var i in data) {
                var val = data[i];

                e.emit('concurrencyInfo', val);
                count++;

                if (data.length === count) {
                    e.emit('endconcurrencyInfo');
                }
            }
        }
        else {
            e.emit('endconcurrencyInfo');
        }
    });

    return (e);
};

var RemoveConcurrencyInfo = function (logKey, data, callback) {
    for (var i in data) {
        redisHandler.GetObj(logKey, data[i], function (err, tempObj) {
            var tagMetaKey = util.format('tagMeta:%s', data[i]);
            redisHandler.GetObj(logKey,tagMetaKey,function(err, reTags){
                if(reTags){
                    commonMethods.ConvertTagStrToArray(reTags, function(slotInfoTags){
                        var obj = JSON.parse(tempObj);
                        //if (obj.ObjKey.search(/^(ConcurrencyInfo)[^\s]*/) != -1) {
                        //    slotInfoTags = ["company_" + obj.Company, "tenant_" + obj.Tenant, "category_" + obj.Category, "resourceid_" + obj.ResourceId, "objtype_ConcurrencyInfo"];
                        //}
                        //else {
                        //    slotInfoTags = ["company_" + obj.Company, "tenant_" + obj.Tenant, "category_" + obj.Category, "state_" + obj.State, "resourceid_" + obj.ResourceId, "objtype_CSlotInfo", "slotid_" + obj.SlotId];
                        //}
                        redisHandler.RemoveObj_V_T(logKey, obj.ObjKey, slotInfoTags, function (err, result) {
                            if (err) {
                                console.log(err);
                            }
                        });
                    });
                }
            });
        });
    }
};

var RemoveResourceState = function (logKey, company, tenant, resourceid, callback) {
    var StateKey = util.format('ResourceState:%d:%d:%s', company, tenant, resourceid);
    redisHandler.RemoveObj(logKey, StateKey, function (err, result) {
        callback(err, result);
    });
};

var AddResource = function (logKey, basicData, callback)  {
    infoLogger.DetailLogger.log('info', '%s ************************* Start AddResource *************************', logKey);
    var accessToken = util.format('%d:%d', basicData.Tenant,basicData.Company);

    var preResourceData = {};
    resourceService.GetResourceDetails(accessToken,basicData.ResourceId,function(resErr, resRes, resObj){
        if(resErr){
            callback(resErr, resRes, preResourceData);
        }else{
            if(resObj.IsSuccess) {
                preResourceData = {
                    Company: resObj.Result.CompanyId,
                    Tenant: resObj.Result.TenantId,
                    Class: resObj.Result.ResClass,
                    Type: resObj.Result.ResType,
                    Category: resObj.Result.ResCategory,
                    ResourceId: resObj.Result.ResourceId.toString(),
                    OtherInfo: resObj.Result.OtherData,
                    ConcurrencyInfo: [],
                    ResourceAttributeInfo: []
                };

                PreProcessResourceData(logKey,accessToken,preResourceData,function(err, msg, preProcessResData){
                    if(err){
                        callback(err, msg, null);
                    }else{
                        var concurrencyInfo = [];
                        var sci = SetConcurrencyInfo(preProcessResData.ConcurrencyInfo);

                        sci.on('concurrencyInfo', function (obj) {
                            //Validate login request with handling type
                            if(basicData.HandlingTypes.indexOf(obj.HandlingType) > -1 ) {
                                //var concurrencySlotInfo = [];
                                for (var i = 0; i < obj.NoOfSlots; i++) {
                                    var slotInfokey = util.format('CSlotInfo:%d:%d:%s:%s:%d', preProcessResData.Company, preProcessResData.Tenant, preProcessResData.ResourceId, obj.HandlingType, i);
                                    var slotInfo = {
                                        Company: preProcessResData.Company,
                                        Tenant: preProcessResData.Tenant,
                                        HandlingType: obj.HandlingType,
                                        State: "Available",
                                        HandlingRequest: "",
                                        LastReservedTime: "",
                                        MaxReservedTime: 0,
                                        ResourceId: preProcessResData.ResourceId,
                                        SlotId: i,
                                        ObjKey: slotInfokey,
                                        OtherInfo: ""
                                    };
                                    var slotInfoTags = ["company_" + slotInfo.Company, "tenant_" + slotInfo.Tenant, "handlingType_" + slotInfo.HandlingType, "state_" + slotInfo.State, "resourceid_" + preProcessResData.ResourceId, "slotid_" + i, "objtype_CSlotInfo"];
                                    concurrencyInfo.push(slotInfokey);

                                    var jsonSlotObj = JSON.stringify(slotInfo);
                                    redisHandler.AddObj_V_T(logKey, slotInfokey, jsonSlotObj, slotInfoTags, function (err, reply, vid) {
                                        if (err) {
                                            console.log(err);
                                        }
                                    });
                                }
                                var cObjkey = util.format('ConcurrencyInfo:%d:%d:%s:%s', preProcessResData.Company, preProcessResData.Tenant, preProcessResData.ResourceId, obj.HandlingType);

                                var tempRefInfoObj = JSON.parse(obj.RefInfo);
                                if (obj.RefInfo) {
                                    tempRefInfoObj.ResourceId = preProcessResData.ResourceId;
                                }
                                var tempRefInfoObjStr = JSON.stringify(tempRefInfoObj);
                                var concurrencyObj = {
                                    Company: preProcessResData.Company,
                                    Tenant: preProcessResData.Tenant,
                                    HandlingType: obj.HandlingType,
                                    LastConnectedTime: "",
                                    RejectCount: 0,
                                    ResourceId: preProcessResData.ResourceId,
                                    ObjKey: cObjkey,
                                    RefInfo: tempRefInfoObjStr
                                };
                                var cObjTags = ["company_" + concurrencyObj.Company, "tenant_" + concurrencyObj.Tenant, "handlingType_" + concurrencyObj.HandlingType, "resourceid_" + preProcessResData.ResourceId, "objtype_ConcurrencyInfo"];
                                concurrencyInfo.push(cObjkey);

                                var jsonConObj = JSON.stringify(concurrencyObj);
                                redisHandler.AddObj_V_T(logKey, cObjkey, jsonConObj, cObjTags, function (err, reply, vid) {
                                    if (err) {
                                        console.log(err);
                                    }
                                });
                            }
                        });

                        sci.on('endconcurrencyInfo', function () {
                            var resourceObj = { Company: preProcessResData.Company, Tenant: preProcessResData.Tenant, Class: preProcessResData.Class, Type: preProcessResData.Type, Category: preProcessResData.Category, ResourceId: preProcessResData.ResourceId, ResourceAttributeInfo: preProcessResData.ResourceAttributeInfo, ConcurrencyInfo: concurrencyInfo, OtherInfo: preProcessResData.OtherInfo };

                            var key = util.format('Resource:%d:%d:%s', resourceObj.Company, resourceObj.Tenant, resourceObj.ResourceId);
                            var tag = ["company_" + resourceObj.Company, "tenant_" + resourceObj.Tenant, "class_" + resourceObj.Class, "type_" + resourceObj.Type, "category_" + resourceObj.Category, "resourceid_" + resourceObj.ResourceId, "objtype_Resource"];

                            var tempAttributeList = [];
                            for (var i in resourceObj.ResourceAttributeInfo) {
                                tempAttributeList.push(resourceObj.ResourceAttributeInfo[i].Attribute);
                            }
                            var sortedAttributes = sortArray.sortData(tempAttributeList);
                            for (var k in sortedAttributes) {
                                tag.push("attribute_" + sortedAttributes[k]);
                            }
                            var jsonObj = JSON.stringify(resourceObj);

                            redisHandler.AddObj_V_T(logKey, key, jsonObj, tag, function (err, reply, vid) {
                                resourceStateMapper.SetResourceState(logKey,resourceObj.Company,resourceObj.Tenant,resourceObj.ResourceId,"Available","Registering",function(err,result){
                                });
                                infoLogger.DetailLogger.log('info', '%s Finished AddResource. Result: %s', logKey, reply);
                                callback(err, reply, vid);
                            });
                        });
                    }
                });
            }else{
                callback(resObj.Exception, resObj.CustomMessage, preResourceData);
            }
        }
    });
};

var ShareResource = function(logKey, basicData, callback){
    infoLogger.DetailLogger.log('info', '%s ************************* Start ShareResource *************************', logKey);
    var accessToken = util.format('%d:%d', basicData.Tenant,basicData.Company);
    var searchTag = ["resourceid_" + basicData.ResourceId, "objtype_Resource"];

    redisHandler.SearchObj_V_T(logKey, searchTag, function (err, strObj) {
        if (err) {
            console.log(err);
            callback(err, null, null);
        }
        else {
            var preResourceData = strObj[0].Obj;
            var cVid = strObj[0].Vid;
            var concurrencyInfo = deepcopy(preResourceData.ConcurrencyInfo);
            PreProcessResourceData(logKey,accessToken,preResourceData,function(err, msg, preProcessResData){
                if(err){
                    callback(err, msg, null);
                }else{
                    var sci = SetConcurrencyInfo(preProcessResData.ConcurrencyInfo);

                    sci.on('concurrencyInfo', function (obj) {
                        //Validate login request with handling type
                        if(basicData.HandlingTypes.indexOf(obj.HandlingType) > -1 ) {
                            //var concurrencySlotInfo = [];
                            var cObjkey = util.format('ConcurrencyInfo:%d:%d:%s:%s', preProcessResData.Company, preProcessResData.Tenant, preProcessResData.ResourceId, obj.HandlingType);
                            redisHandler.CheckObjExists(logKey,cObjkey,function(cErr, isExists){
                                if(cErr){
                                    console.log(cErr);
                                    isExists = 0;
                                }

                                var tempRefInfoObj = JSON.parse(obj.RefInfo);
                                if (obj.RefInfo) {
                                    tempRefInfoObj.ResourceId = preProcessResData.ResourceId;
                                }
                                var tempRefInfoObjStr = JSON.stringify(tempRefInfoObj);
                                var concurrencyObj = {
                                    Company: preProcessResData.Company,
                                    Tenant: preProcessResData.Tenant,
                                    HandlingType: obj.HandlingType,
                                    LastConnectedTime: "",
                                    RejectCount: 0,
                                    ResourceId: preProcessResData.ResourceId,
                                    ObjKey: cObjkey,
                                    RefInfo: tempRefInfoObjStr
                                };
                                var cObjTags = ["company_" + basicData.Company, "tenant_" + basicData.Tenant, "handlingType_" + concurrencyObj.HandlingType, "resourceid_" + preProcessResData.ResourceId, "objtype_ConcurrencyInfo"];

                                var jsonConObj = JSON.stringify(concurrencyObj);
                                if (isExists == 0) {
                                    concurrencyInfo.push(cObjkey);
                                    redisHandler.AddObj_V_T(logKey, cObjkey, jsonConObj, cObjTags, function (err, reply, vid) {
                                        if (err) {
                                            console.log(err);
                                        }
                                    });
                                }else{
                                    var tagMetaKey = util.format('tagMeta:%s', cObjkey);
                                    redisHandler.GetObj(logKey,tagMetaKey,function(err, ceTags){
                                        if(ceTags){
                                            var newCompany = util.format('company_%s',basicData.Company);
                                            commonMethods.AppendNewCompanyTagStr(ceTags, newCompany, function(newTags){
                                                redisHandler.SetTags(logKey, newTags,cObjkey,function(err, reply){
                                                    if (err) {
                                                        console.log(err);
                                                    }
                                                } );
                                            });
                                        }
                                    });
                                }


                                for (var i = 0; i < obj.NoOfSlots; i++) {
                                    var slotInfokey = util.format('CSlotInfo:%d:%d:%s:%s:%d', preProcessResData.Company, preProcessResData.Tenant, preProcessResData.ResourceId, obj.HandlingType, i);
                                    var slotInfo = {
                                        Company: preProcessResData.Company,
                                        Tenant: preProcessResData.Tenant,
                                        HandlingType: obj.HandlingType,
                                        State: "Available",
                                        HandlingRequest: "",
                                        LastReservedTime: "",
                                        MaxReservedTime: 0,
                                        ResourceId: preProcessResData.ResourceId,
                                        SlotId: i,
                                        ObjKey: slotInfokey,
                                        OtherInfo: ""
                                    };
                                    var slotInfoTags = ["company_" + basicData.Company, "tenant_" + basicData.Tenant, "handlingType_" + slotInfo.HandlingType, "state_" + slotInfo.State, "resourceid_" + preProcessResData.ResourceId, "slotid_" + i, "objtype_CSlotInfo"];

                                    var jsonSlotObj = JSON.stringify(slotInfo);
                                    if (isExists == 0) {
                                        concurrencyInfo.push(slotInfokey);
                                        redisHandler.AddObj_V_T(logKey, slotInfokey, jsonSlotObj, slotInfoTags, function (err, reply, vid) {
                                            if (err) {
                                                console.log(err);
                                            }
                                        });
                                    }else{
                                        var slotTagMetaKey = util.format('tagMeta:%s', slotInfokey);
                                        redisHandler.GetObj(logKey,slotTagMetaKey,function(err, seTags){
                                            if(seTags){
                                                var newCompany = util.format('company_%s',basicData.Company);
                                                commonMethods.AppendNewCompanyTagStr(seTags, newCompany, function(newsTags){
                                                    redisHandler.SetTags(logKey, newsTags,slotInfokey,function(err, reply){
                                                        if (err) {
                                                            console.log(err);
                                                        }
                                                    } );
                                                });
                                            }
                                        });
                                    }
                                }
                            });
                        }
                    });

                    sci.on('endconcurrencyInfo', function () {
                        var resourceObj = { Company: preProcessResData.Company, Tenant: preProcessResData.Tenant, Class: preProcessResData.Class, Type: preProcessResData.Type, Category: preProcessResData.Category, ResourceId: preProcessResData.ResourceId, ResourceAttributeInfo: preProcessResData.ResourceAttributeInfo, ConcurrencyInfo: concurrencyInfo, OtherInfo: preProcessResData.OtherInfo };

                        var key = util.format('Resource:%d:%d:%s', resourceObj.Company, resourceObj.Tenant, resourceObj.ResourceId);
                        var tag = ["company_" + resourceObj.Company, "tenant_" + resourceObj.Tenant, "class_" + resourceObj.Class, "type_" + resourceObj.Type, "category_" + resourceObj.Category, "resourceid_" + resourceObj.ResourceId, "objtype_Resource"];

                        var tagMetaKey = util.format('tagMeta:%s', key);
                        redisHandler.GetObj(logKey,tagMetaKey,function(err, reTags){
                            if(reTags){
                                var newCompany = util.format('company_%s',basicData.Company);
                                commonMethods.AppendNewCompanyTagArray(reTags, newCompany, function(newTags){
                                    tag = newTags;
                                    var tempAttributeList = [];
                                    for (var i in resourceObj.ResourceAttributeInfo) {
                                        tempAttributeList.push(resourceObj.ResourceAttributeInfo[i].Attribute);
                                    }
                                    var sortedAttributes = sortArray.sortData(tempAttributeList);
                                    for (var k in sortedAttributes) {
                                        tag.push("attribute_" + sortedAttributes[k]);
                                    }
                                    var jsonObj = JSON.stringify(resourceObj);

                                    redisHandler.SetObj_V_T(logKey, key, jsonObj, tag, cVid.toString(), function (err, reply, vid) {
                                        infoLogger.DetailLogger.log('info', '%s Finished SetResource. Result: %s', logKey, reply);
                                        callback(err, reply, vid);
                                    });
                                });
                            }
                        });
                    });
                }
            });
        }
    });
};

var RemoveResource = function (logKey, company, tenant, resourceId, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start RemoveResource *************************', logKey);

    var key = util.format('Resource:%s:%s:%s', company, tenant, resourceId);
    redisHandler.GetObj(logKey, key, function (err, obj) {
        if (err) {
            callback(err, "false");
        }
        else {

            var resourceObj = JSON.parse(obj);
            RemoveConcurrencyInfo(logKey, resourceObj.ConcurrencyInfo, function () {
            });
            RemoveResourceState(logKey, resourceObj.Company, resourceObj.Tenant, resourceObj.ResourceId, function () {
            });

            var tag = ["company_" + resourceObj.Company, "tenant_" + resourceObj.Tenant, "class_" + resourceObj.Class, "type_" + resourceObj.Type, "category_" + resourceObj.Category, "objtype_Resource", "resourceid_" + resourceObj.ResourceId];
            var tagMetaKey = util.format('tagMeta:%s', key);
            redisHandler.GetObj(logKey,tagMetaKey,function(err, reTags){
                if(reTags){
                    var newCompany = util.format('company_%s',company);
                    commonMethods.AppendNewCompanyTagArray(reTags, newCompany, function(newTags){
                        tag = newTags;
                    });
                }
            });

            //var tempAttributeList = [];
            //for (var i in resourceObj.ResourceAttributeInfo) {
            //    tempAttributeList.push(resourceObj.ResourceAttributeInfo[i].Attribute);
            //}
            //var sortedAttributes = sortArray.sortData(tempAttributeList);
            //for (var k in sortedAttributes) {
            //    tag.push("attribute_" + sortedAttributes[k]);
            //}

            redisHandler.RemoveObj_V_T(logKey, key, tag, function (err, result) {
                if (err) {
                    infoLogger.DetailLogger.log('info', '%s Finished RemoveResource. Result: %s', logKey, "false");
                    callback(err, "false");
                }
                else {
                    infoLogger.DetailLogger.log('info', '%s Finished RemoveResource. Result: %s', logKey, result);
                    callback(null, result);
                }
            });
        }
    });
};

var RemoveShareResource = function (logKey, company, tenant, resourceId, handlingTypes, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start RemoveShareResource *************************', logKey);
    var accessToken = util.format('%s:%s', tenant,company);
    var searchTag = ["resourceid_" + resourceId, "objtype_Resource"];

    redisHandler.SearchObj_V_T(logKey, searchTag, function (err, strObj) {
        if (err) {
            console.log(err);
            callback(err, null, null);
        }
        else {
            var preResourceData = strObj[0].Obj;
            var cVid = strObj[0].Vid;
            var concurrencyInfo = deepcopy(preResourceData.ConcurrencyInfo);
            PreProcessResourceData(logKey,accessToken,preResourceData,function(err, msg, preProcessResData, attributeToRemove){
                if(err){
                    callback(err, msg, null);
                }else{
                    var sci = SetConcurrencyInfo(preProcessResData.ConcurrencyInfo);

                    sci.on('concurrencyInfo', function (obj) {
                        //Validate login request with handling type
                        if(handlingTypes.indexOf(obj.HandlingType) > -1 ) {
                            //var concurrencySlotInfo = [];
                            var cObjkey = util.format('ConcurrencyInfo:%d:%d:%s:%s', preProcessResData.Company, preProcessResData.Tenant, preProcessResData.ResourceId, obj.HandlingType);
                            redisHandler.CheckObjExists(logKey,cObjkey,function(cErr, isExists){
                                if(cErr){
                                    console.log(cErr);
                                    isExists = 0;
                                }
                                if (isExists == 1) {
                                    var tagMetaKey = util.format('tagMeta:%s', cObjkey);
                                    redisHandler.GetObj(logKey,tagMetaKey,function(err, ceTags){
                                        if(ceTags){
                                            var newCompany = util.format('company_%s',company);
                                            commonMethods.RemoveTagFromTagStr(ceTags, newCompany, function(newTags){
                                                redisHandler.SetTags(logKey, newTags,cObjkey,function(err, reply){
                                                    if (err) {
                                                        console.log(err);
                                                    }
                                                } );
                                            });
                                        }
                                    });
                                }

                                for (var i = 0; i < obj.NoOfSlots; i++) {
                                    var slotInfokey = util.format('CSlotInfo:%d:%d:%s:%s:%d', preProcessResData.Company, preProcessResData.Tenant, preProcessResData.ResourceId, obj.HandlingType, i);

                                    if (isExists == 1) {
                                        var slotTagMetaKey = util.format('tagMeta:%s', slotInfokey);
                                        redisHandler.GetObj(logKey,slotTagMetaKey,function(err, seTags){
                                            if(seTags){
                                                var newCompany = util.format('company_%s',company);
                                                commonMethods.RemoveTagFromTagStr(seTags, newCompany, function(newsTags){
                                                    redisHandler.SetTags(logKey, newsTags,slotInfokey,function(err, reply){
                                                        if (err) {
                                                            console.log(err);
                                                        }
                                                    } );
                                                });
                                            }
                                        });
                                    }
                                }
                            });
                        }
                    });

                    sci.on('endconcurrencyInfo', function () {
                        var resourceObj = { Company: preProcessResData.Company, Tenant: preProcessResData.Tenant, Class: preProcessResData.Class, Type: preProcessResData.Type, Category: preProcessResData.Category, ResourceId: preProcessResData.ResourceId, ResourceAttributeInfo: preProcessResData.ResourceAttributeInfo, ConcurrencyInfo: concurrencyInfo, OtherInfo: preProcessResData.OtherInfo };

                        var key = util.format('Resource:%d:%d:%s', resourceObj.Company, resourceObj.Tenant, resourceObj.ResourceId);

                        var tagMetaKey = util.format('tagMeta:%s', key);
                        redisHandler.GetObj(logKey,tagMetaKey,function(err, reTags){
                            if(reTags){
                                var tagsToRemove = [util.format('company_%s',company)];

                                for (var i in attributeToRemove) {
                                    tagsToRemove.push("attribute_" + attributeToRemove[i].Attribute);
                                    resourceObj.ResourceAttributeInfo = commonMethods.RemoveItemFromObjectArray(resourceObj.ResourceAttributeInfo, 'Attribute', attributeToRemove[i].Attribute);
                                }

                                commonMethods.RemoveTagsFromTagArray(reTags, tagsToRemove, function(newTags){
                                    var jsonObj = JSON.stringify(resourceObj);

                                    redisHandler.SetObj_V_T(logKey, key, jsonObj, newTags, cVid.toString(), function (err, reply, vid) {
                                        infoLogger.DetailLogger.log('info', '%s Finished SetResource. Result: %s', logKey, reply);
                                        callback(err, reply, vid);
                                    });
                                });
                            }
                        });
                    });
                }
            });
        }
    });
};

var SetResource = function (logKey, basicObj, cVid, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start SetResource *************************', logKey);

    var key = util.format('Resource:%d:%d:%s', basicObj.Company, basicObj.Tenant, basicObj.ResourceId);

    redisHandler.GetObj(logKey, key, function (err, jobj) {
        if (err) {
            console.log(err);
        }
        else {
            var obj = JSON.parse(jobj);
            var resourceObj = { Company: basicData.Company, Tenant: basicData.Tenant, Class: basicData.Class, Type: basicData.Type, Category: basicData.Category, ResourceId: basicData.ResourceId, ResourceAttributeInfo: basicData.ResourceAttributeInfo, ConcurrencyInfo: obj.ConcurrencyInfo, State: obj.State };

            var tag = ["company_" + resourceObj.Company, "tenant_" + resourceObj.Tenant, "class_" + resourceObj.Class, "type_" + resourceObj.Type, "category_" + resourceObj.Category, "objtype_Resource", "resourceid_" + resourceObj.ResourceId];

            var tagMetaKey = util.format('tagMeta:%s', key);
            redisHandler.GetObj(logKey,tagMetaKey,function(err, reTags){
                if(reTags){
                    var newCompany = util.format('company_%s',basicData.Company);
                    commonMethods.AppendNewCompanyTagArray(reTags, newCompany, function(newTags){
                        tag = newTags;
                    });
                }
            });

            var tempAttributeList = [];
            for (var i in resourceObj.ResourceAttributeInfo) {
                tempAttributeList.push(resourceObj.ResourceAttributeInfo[i].Attribute);
            }
            var sortedAttributes = sortArray.sortData(tempAttributeList);
            for (var k in sortedAttributes) {
                tag.push("attribute_" + sortedAttributes[k]);
            }
            var jsonObj = JSON.stringify(resourceObj);

            redisHandler.SetObj_V_T(logKey, key, jsonObj, tag, cVid, function (err, reply, vid) {
                infoLogger.DetailLogger.log('info', '%s Finished SetResource. Result: %s', logKey, reply);
                callback(err, reply, vid);
            });
        }
    });
};

var GetResource = function (logKey, company, tenant, resourceId, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start GetResource *************************', logKey);

    var key = util.format('Resource:%s:%s:%s', company, tenant, resourceId);
    redisHandler.GetObj_V(logKey, key, function (err, result, vid) {
        infoLogger.DetailLogger.log('info', '%s Finished GetResource. Result: %s', logKey, result);
        callback(err, result, vid);
    });
};

var GetResourceState = function (logKey, company, tenant, resourceId, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start GetResourceState *************************', logKey);

    var key = util.format('ResourceState:%s:%s:%s', company, tenant, resourceId);
    redisHandler.GetObj(logKey, key, function (err, result) {
        infoLogger.DetailLogger.log('info', '%s Finished GetResourceState. Result: %s', logKey, result);
        callback(err, result);
    });
};

var SearchResourcebyTags = function (logKey, tags, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start SearchResourcebyTags *************************', logKey);

    if (Array.isArray(tags)) {
        tags.push("objtype_Resource");
        redisHandler.SearchObj_V_T(logKey, tags, function (err, result) {
            infoLogger.DetailLogger.log('info', '%s Finished SearchResourcebyTags. Result: %s', logKey, result);
            callback(err, result);
        });
    }
    else {
        var e = new Error();
        e.message = "tags must be a string array";
        infoLogger.DetailLogger.log('info', '%s Finished SearchResourcebyTags. Result: %s', logKey, "tags must be a string array");
        callback(e, null);
    }
};

var UpdateLastConnectedTime = function (logKey, company, tenant, handlingType, resourceid, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start UpdateLastConnectedTime *************************', logKey);

    var cObjkey = util.format('ConcurrencyInfo:%d:%d:%s:%s', company, tenant, resourceid, handlingType);
    var date = new Date();

    redisHandler.GetObj_V(logKey, cObjkey, function (err, obj, vid) {
        if (err) {
            console.log(err);
        }
        else {
            var tagMetaKey = util.format('tagMeta:%s', cObjkey);
            redisHandler.GetObj(logKey,tagMetaKey,function(err, ceTags){
                if(err){
                    callback(err, null, null);
                }
                if(ceTags){
                    commonMethods.GetSortedCompanyTagArray(ceTags, function(companyTags){
                        var cObj = JSON.parse(obj);
                        cObj.LastConnectedTime = date.toISOString();
                        cObj.RejectCount = 0;
                        var jCObj = JSON.stringify(cObj);
                        var tags = ["tenant_" + cObj.Tenant, "handlingType_" + cObj.HandlingType, "resourceid_" + cObj.ResourceId, "objtype_ConcurrencyInfo"];
                        var cObjTags = companyTags.concat(tags);
                        redisHandler.SetObj_V_T(logKey, cObjkey, jCObj, cObjTags, vid, function () {
                            infoLogger.DetailLogger.log('info', '%s Finished UpdateLastConnectedTime. Result: %s', logKey, result);
                            callback(err, result, vid);
                        });
                    });
                }else{
                    callback(new Error("Update Redis tags failed"), null, null);
                }
            });

        }
    });
};

var UpdateRejectCount = function (logKey, company, tenant, handlingType, resourceid, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start UpdateRejectCount *************************', logKey);

    var cObjkey = util.format('ConcurrencyInfo:%d:%d:%s:%s', company, tenant, resourceid, handlingType);
    var date = new Date();

    redisHandler.GetObj_V(logKey, cObjkey, function (err, obj, vid) {
        if (err) {
            console.log(err);
        }
        else {
            var tagMetaKey = util.format('tagMeta:%s', cObjkey);
            redisHandler.GetObj(logKey,tagMetaKey,function(err, ceTags){
                if(err){
                    callback(err, null, null);
                }
                if(ceTags){
                    commonMethods.GetSortedCompanyTagArray(ceTags, function(companyTags){
                        var cObj = JSON.parse(obj);
                        cObj.RejectCount = cObj.RejectCount + 1;
                        var jCObj = JSON.stringify(cObj);
                        var tags = ["tenant_" + cObj.Tenant, "handlingType_" + cObj.HandlingType, "resourceid_" + cObj.ResourceId, "objtype_ConcurrencyInfo"];
                        var cObjTags = companyTags.concat(tags);
                        //todo
                        //if RejectCount > max reject count, update notification service

                        redisHandler.SetObj_V_T(logKey, cObjkey, jCObj, cObjTags, vid, function () {
                            infoLogger.DetailLogger.log('info', '%s Finished UpdateRejectCount. Result: %s', logKey, result);
                            callback(err, result, vid);
                        });
                    });
                }else{
                    callback(new Error("Update Redis tags failed"), null, null);
                }
            });
        }
    });
};

var UpdateSlotStateAvailable = function (logKey, company, tenant, handlingType, resourceid, slotid, reason, otherInfo, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start UpdateSlotStateAvailable *************************', logKey);

    var slotInfokey = util.format('CSlotInfo:%s:%s:%s:%s:%s', company, tenant, resourceid, handlingType, slotid);
    redisHandler.GetObj_V(logKey, slotInfokey, function (err, obj, vid) {
        if (err) {
            console.log(err);
            callback(err, false);
        }
        else {
            var tagMetaKey = util.format('tagMeta:%s', slotInfokey);
            redisHandler.GetObj(logKey,tagMetaKey,function(err, ceTags){
                if(err){
                    callback(err, null, null);
                }
                if(ceTags){
                    commonMethods.GetSortedCompanyTagArray(ceTags, function(companyTags){
                        var tempObj = JSON.parse(obj);
                        if (otherInfo == "Reject") {
                            UpdateRejectCount(logKey, tempObj.Company, tempObj.Tenant, tempObj.HandlingType, tempObj.ResourceId, function () { });
                            var pubMessage = util.format("EVENT:%s:%s:%s:%s:%s:%s:%s:%s:YYYY", company, tenant, "ARDS", "REQUEST", "REJECT", reason, tempObj.ResourceId, sessionId);
                            redisHandler.Publish(logKey, "events", pubMessage, function(){});
                        }
                        var handledRequest = tempObj.HandlingRequest;
                        tempObj.State = "Available";
                        tempObj.HandlingRequest = "";
                        tempObj.OtherInfo = "";
                        tempObj.MaxReservedTime = 0;
                        var tags = ["tenant_" + tempObj.Tenant, "handlingType_" + tempObj.HandlingType, "state_" + tempObj.State, "resourceid_" + tempObj.ResourceId, "slotid_" + tempObj.SlotId, "objtype_CSlotInfo"];
                        var slotInfoTags = companyTags.concat(tags);
                        var jsonObj = JSON.stringify(tempObj);
                        redisHandler.SetObj_V_T(logKey, slotInfokey, jsonObj, slotInfoTags, vid, function (err, reply, vid) {
                            infoLogger.DetailLogger.log('info', '%s Finished UpdateSlotStateAvailable. Result: %s', logKey, reply);
                            if (err != null) {
                                console.log(err);
                            }
                            else {
                                var internalAccessToken = util.format('%s:%s', tenant,company);
                                resourceService.AddResourceStatusChangeInfo(internalAccessToken, tempObj.ResourceId, "SloatStatus", tempObj.State, otherInfo, handledRequest, function(err, result, obj){
                                    if(err){
                                        console.log("AddResourceStatusChangeInfo Failed.", err);
                                    }else{
                                        console.log("AddResourceStatusChangeInfo Success.", obj);
                                    }
                                });
                            }
                            callback(err, reply);
                        });
                    });
                }else{
                    callback(new Error("Update Redis tags failed"), null, null);
                }
            });
        }
    });
};

var UpdateSlotStateReserved = function (logKey, company, tenant, handlingType, resourceid, slotid, sessionid, maxReservedTime, otherInfo, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start UpdateSlotStateReserved *************************', logKey);

    var slotInfokey = util.format('CSlotInfo:%s:%s:%s:%s:%s', company, tenant, resourceid, handlingType, slotid);
    redisHandler.GetObj_V(logKey, slotInfokey, function (err, obj, vid) {
        if (err) {
            console.log(err);
            callback(err, false);
        }
        else {
            var tagMetaKey = util.format('tagMeta:%s', slotInfokey);
            redisHandler.GetObj(logKey,tagMetaKey,function(err, ceTags){
                if(err){
                    callback(err, null, null);
                }
                if(ceTags){
                    commonMethods.GetSortedCompanyTagArray(ceTags, function(companyTags){
                        var date = new Date();
                        var tempObj = JSON.parse(obj);
                        tempObj.State = "Reserved";
                        tempObj.HandlingRequest = sessionid;
                        tempObj.LastReservedTime = date.toISOString();
                        tempObj.OtherInfo = otherInfo;
                        tempObj.MaxReservedTime = maxReservedTime;
                        var tags = ["tenant_" + tempObj.Tenant, "handlingType_" + tempObj.HandlingType, "state_" + tempObj.State, "resourceid_" + tempObj.ResourceId, "slotid_" + tempObj.SlotId, "handlingrequest_" + tempObj.HandlingRequest, "objtype_CSlotInfo"];
                        var slotInfoTags = companyTags.concat(tags);
                        var jsonObj = JSON.stringify(tempObj);
                        redisHandler.SetObj_V_T(logKey, slotInfokey, jsonObj, slotInfoTags, vid, function (err, reply, vid) {
                            infoLogger.DetailLogger.log('info', '%s Finished UpdateSlotStateReserved. Result: %s', logKey, reply);
                            if (err != null) {
                                console.log(err);
                            }
                            else {
                                var internalAccessToken = util.format('%s:%s', tenant,company);
                                resourceService.AddResourceStatusChangeInfo(internalAccessToken, tempObj.ResourceId, "SloatStatus", tempObj.State, otherInfo, sessionid, function(err, result, obj){
                                    if(err){
                                        console.log("AddResourceStatusChangeInfo Failed.", err);
                                    }else{
                                        console.log("AddResourceStatusChangeInfo Success.", obj);
                                    }
                                });
                            }
                            callback(err, reply);
                        });
                    });
                }else{
                    callback(new Error("Update Redis tags failed"), null, null);
                }
            });
        }
    });
};

var UpdateSlotStateConnected = function (logKey, company, tenant, handlingType, resourceid, slotid, sessionid, otherInfo, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start UpdateSlotStateConnected *************************', logKey);

    var slotInfokey = util.format('CSlotInfo:%s:%s:%s:%s:%s', company, tenant, resourceid, handlingType, slotid);
    redisHandler.GetObj_V(logKey, slotInfokey, function (err, obj, vid) {
        if (err) {
            console.log(err);
            callback(err, false);
        }
        else {
            var tagMetaKey = util.format('tagMeta:%s', slotInfokey);
            redisHandler.GetObj(logKey,tagMetaKey,function(err, ceTags){
                if(err){
                    callback(err, null, null);
                }
                if(ceTags){
                    commonMethods.GetSortedCompanyTagArray(ceTags, function(companyTags){
                        var tempObj = JSON.parse(obj);
                        tempObj.State = "Connected";
                        tempObj.HandlingRequest = sessionid;
                        tempObj.OtherInfo = otherInfo;
                        var tags = ["tenant_" + tempObj.Tenant, "handlingType_" + tempObj.HandlingType, "state_" + tempObj.State, "resourceid_" + tempObj.ResourceId, "slotid_" + tempObj.SlotId, "handlingrequest_" + tempObj.HandlingRequest, "objtype_CSlotInfo"];
                        var slotInfoTags = companyTags.concat(tags);
                        var jsonObj = JSON.stringify(tempObj);
                        redisHandler.SetObj_V_T(logKey, slotInfokey, jsonObj, slotInfoTags, vid, function (err, reply, vid) {
                            if (!err) {
                                UpdateLastConnectedTime(logKey, tempObj.Company, tempObj.Tenant, tempObj.HandlingType, resourceid, function () { });
                            }
                            infoLogger.DetailLogger.log('info', '%s Finished UpdateSlotStateConnected. Result: %s', logKey, reply);
                            if (err != null) {
                                console.log(err);
                            }
                            else {
                                var internalAccessToken = util.format('%s:%s', tenant,company);
                                resourceService.AddResourceStatusChangeInfo(internalAccessToken, tempObj.ResourceId, "SloatStatus", tempObj.State, otherInfo, sessionid, function(err, result, obj){
                                    if(err){
                                        console.log("AddResourceStatusChangeInfo Failed.", err);
                                    }else{
                                        console.log("AddResourceStatusChangeInfo Success.", obj);
                                    }
                                });
                            }
                            callback(err, reply);
                        });
                    });
                }else{
                    callback(new Error("Update Redis tags failed"), null, null);
                }
            });
        }
    });
};

var UpdateSlotStateCompleted = function(logKey, company, tenant, handlingType, resourceid, slotid, sessionid, otherInfo, callback){
    setTimeout(UpdateSlotStateAvailable(logKey, company, tenant, handlingType, resourceid, slotid, otherInfo, function (err, result) {

    }), 10000);
    if (err != null) {
        console.log(err);
    }
    else {
        var internalAccessToken = util.format('%s:%s', tenant,company);
        resourceService.AddResourceStatusChangeInfo(internalAccessToken, resourceid, "SloatStatus", "Completed", otherInfo, sessionid, function(err, result, obj){
            if(err){
                console.log("AddResourceStatusChangeInfo Failed.", err);
            }else{
                console.log("AddResourceStatusChangeInfo Success.", obj);
            }
        });
    }
    callback(err, "OK");
};

var UpdateSlotStateBySessionId = function (logKey, company, tenant, handlingType, resourceid, sessionid, state, reason, otherInfo, callback) {
    var slotInfoTags = [];

    if (resourceid == "") {
        slotInfoTags = ["company_" + company, "tenant_" + tenant, "handlingType_" + handlingType, "handlingrequest_" + sessionid];
    }
    else {
        slotInfoTags = ["company_" + company, "tenant_" + tenant, "handlingType_" + handlingType, "resourceid_" + resourceid, "handlingrequest_" + sessionid];
    }

    SearchCSlotByTags(logKey, slotInfoTags, function (err, cslots) {
        if (err) {
            console.log(err);
            callback(err, null);
        }
        else {
            if (cslots.length > 0) {
                for (var i in cslots) {
                    var cs = cslots[i].Obj;
                    if (cs.HandlingRequest == sessionid) {
                        switch (state) {
                            case "Available":
                                UpdateSlotStateAvailable(logKey, cs.Company, cs.Tenant, cs.HandlingType, cs.ResourceId, cs.SlotId, reason, otherInfo, function (err, result) {
                                    callback(err, result);
                                });
                                break;

                            case "Connected":
                                UpdateSlotStateConnected(logKey, cs.Company, cs.Tenant, cs.HandlingType, cs.ResourceId, cs.SlotId, sessionid, otherInfo, function (err, result) {
                                    callback(err, result);
                                });
                                break;

                            case "Completed":
                                UpdateSlotStateCompleted(logKey, cs.Company, cs.Tenant, cs.HandlingType, cs.ResourceId, cs.SlotId, sessionid, otherInfo, function (err, result){
                                    callback(err, result);
                                });
                                break;
                            default :
                                callback(err, "Invalied Request");
                        }
                    }
                }
            }
            else {
                callback(err, "No Recerved Resource CSlot found for sessionId: " + sessionid);
            }
        }
    });
};

var SearchCSlotByTags = function (logKey, tags, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start SearchCSlotByTags *************************', logKey);

    if (Array.isArray(tags)) {
        tags.push("objtype_CSlotInfo");
        redisHandler.SearchObj_V_T(logKey, tags, function (err, result) {
            infoLogger.DetailLogger.log('info', '%s Finished SearchCSlotByTags. Result: %s', logKey, result);
            callback(err, result);
        });
    }
    else {
        var e = new Error();
        e.message = "tags must be a string array";
        infoLogger.DetailLogger.log('info', '%s Finished SearchCSlotByTags. Result: %s', logKey, "tags must be a string array");
        callback(e, null);
    }
};

var SearchConcurrencyInfoByTags = function (logKey, tags, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start SearchConcurrencyInfoByTags *************************', logKey);

    if (Array.isArray(tags)) {
        tags.push("objtype_ConcurrencyInfo");
        redisHandler.SearchObj_V_T(tags, function (err, result) {
            infoLogger.DetailLogger.log('info', '%s Finished SearchConcurrencyInfoByTags. Result: %s', logKey, result);
            callback(err, result);
        });
    }
    else {
        var e = new Error();
        e.message = "tags must be a string array";
        infoLogger.DetailLogger.log('info', '%s Finished SearchConcurrencyInfoByTags. Result: %s', logKey, "tags must be a string array");
        callback(e, null);
    }
};

var DoResourceSelection = function (company, tenant,resourceCount, sessionId, serverType, requestType, selectionAlgo, handlingAlgo, otherInfo, callback) {


    var rUrl = util.format('http://%s',config.Services.routingServiceHost)

    if(validator.isIP(config.Services.routingServiceHost)) {
        rUrl = util.format('http://%s:%s', config.Services.routingServiceHost, config.Services.routingServicePort)
    }
    var params = util.format('/resourceselection/getresource/%d/%d/%d/%s/%s/%s/%s/%s/%s', company, tenant, resourceCount, sessionId, serverType, requestType, selectionAlgo, handlingAlgo, otherInfo);
    restClientHandler.DoGet(rUrl, params, function (err, res, obj) {
        callback(err, res, obj);
    });
};

module.exports.AddResource = AddResource;
module.exports.SetResource = SetResource;
module.exports.ShareResource = ShareResource;
module.exports.RemoveResource = RemoveResource;
module.exports.RemoveShareResource = RemoveShareResource;
module.exports.GetResource = GetResource;
module.exports.GetResourceState = GetResourceState;
module.exports.SearchResourcebyTags = SearchResourcebyTags;

module.exports.UpdateLastConnectedTime = UpdateLastConnectedTime;
module.exports.UpdateSlotStateAvailable = UpdateSlotStateAvailable;
module.exports.UpdateSlotStateReserved = UpdateSlotStateReserved;
module.exports.UpdateSlotStateConnected = UpdateSlotStateConnected;
module.exports.UpdateSlotStateBySessionId = UpdateSlotStateBySessionId;
module.exports.SearchCSlotByTags = SearchCSlotByTags;
module.exports.SearchConcurrencyInfoByTags = SearchConcurrencyInfoByTags;

module.exports.DoResourceSelection = DoResourceSelection;