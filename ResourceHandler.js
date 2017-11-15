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
var notificationService = require('./services/notificationService');
var deepcopy = require('deepcopy');
var moment = require('moment');
var async = require('async');
var ardsMonitoringService = require('./services/ardsMonitoringService');
var scheduleWorkerHandler = require('./ScheduleWorkerHandler');

var SetProductivityData = function (logKey, company, tenant, resourceId, eventType) {
    try {
        console.log("Start SetProductivityData:: eventType: " + eventType);
        var slotInfoTags = ["company_" + company, "tenant_" + tenant, "state_Connected", "resourceid_" + resourceId];
        SearchCSlotByTags(logKey, slotInfoTags, function (err, cslots) {
            if (err) {
                console.log(err);
            }
            else {
                var pubMessage;
                var productiveItems = [];
                for (var i in cslots) {
                    var cs = cslots[i].Obj;
                    if (cs.EnableToProductivity) {
                        productiveItems.push(cs);
                        console.log("Found productiveItem: " + cs.HandlingType);
                    }
                }
                switch (eventType) {
                    case "Connected":
                        if (productiveItems.length === 1) {
                            pubMessage = util.format("EVENT:%s:%s:%s:%s:%s:%s:%s:%s:YYYY", tenant, company, "Resource", "Productivity", "StartWorking", resourceId, "param2", resourceId);
                            console.log("Start publish Message: " + pubMessage);
                            redisHandler.Publish("DashBoardEvent", "events", pubMessage, function () {
                            });
                        }
                        break;
                    case "Completed":
                        if (productiveItems.length === 0) {
                            pubMessage = util.format("EVENT:%s:%s:%s:%s:%s:%s:%s:%s:YYYY", tenant, company, "Resource", "Productivity", "EndWorking", resourceId, "param2", resourceId);
                            console.log("Start publish Message: " + pubMessage);
                            redisHandler.Publish("DashBoardEvent", "events", pubMessage, function () {
                            });
                        }
                        break;
                    default :
                        break;
                }
            }
        });
    } catch (ex) {
        console.log("SetProductivityData Failed:: " + ex);
    }
};

var PreProcessTaskData = function (accessToken, taskInfos, loginTask) {
    var e = new EventEmitter();
    process.nextTick(function () {
        if (Array.isArray(taskInfos) && taskInfos.length > 0) {
            var count = 0;
            for (var i in taskInfos) {
                var taskInfo = taskInfos[i];
                var attributes = [];
                var validateHandlingType = commonMethods.FilterByID(loginTask, "Type", taskInfo.ResTask.ResTaskInfo.TaskType);
                if (validateHandlingType) {
                    resourceService.GetResourceAttributeDetails(accessToken, taskInfo, function (resAttErr, resAttRes, resAttObj, reTaskInfo) {
                        var task = {
                            HandlingType: reTaskInfo.ResTask.ResTaskInfo.TaskType,
                            EnableToProductivity: reTaskInfo.ResTask.AddToProductivity,
                            NoOfSlots: reTaskInfo.Concurrency,
                            RefInfo: reTaskInfo.RefInfo
                        };
                        if (resAttErr) {
                            count++;
                            consile.log(resAttErr);
                            e.emit('taskInfo', task, attributes);
                            if (taskInfos.length === count) {
                                e.emit('endTaskInfo');
                            }
                        } else {
                            var ppad = PreProcessAttributeData(task.HandlingType, resAttObj.Result.ResResourceAttributeTask);
                            ppad.on('attributeInfo', function (attribute) {
                                attributes.push(attribute);
                            });
                            ppad.on('endAttributeInfo', function () {
                                count++;
                                e.emit('taskInfo', task, attributes);
                                if (taskInfos.length === count) {
                                    e.emit('endTaskInfo');
                                }
                            });
                        }
                    });
                } else {
                    count++;
                    if (taskInfos.length === count) {
                        e.emit('endTaskInfo');
                    }
                }
            }
        }
        else {
            e.emit('endTaskInfo');
        }
    });

    return (e);
};

var PreProcessAttributeData = function (handlingType, attributeInfos) {
    var e = new EventEmitter();
    process.nextTick(function () {
        if (Array.isArray(attributeInfos) && attributeInfos.length > 0) {
            var count = 0;
            for (var i in attributeInfos) {
                var attributeInfo = attributeInfos[i];
                count++;

                if (attributeInfo && attributeInfo.Percentage && attributeInfo.Percentage > 0) {
                    var attribute = {
                        Attribute: attributeInfo.AttributeId.toString(),
                        HandlingType: handlingType,
                        Percentage: attributeInfo.Percentage
                    };
                    e.emit('attributeInfo', attribute);
                }

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

var PreProcessResourceData = function (logKey, accessToken, preResourceData, loginTask, callback) {
    resourceService.GetResourceTaskDetails(accessToken, preResourceData.ResourceId, function (taskErr, taskRes, taskObj) {
        var newAttributeInfo = [];
        if (taskErr) {
            callback(taskErr, taskRes, preResourceData, newAttributeInfo);
        } else {
            if (taskObj.IsSuccess) {
                var pptd = PreProcessTaskData(accessToken, taskObj.Result, loginTask);
                pptd.on('taskInfo', function (taskInfo, attributeInfo) {
                    preResourceData.ConcurrencyInfo.push(taskInfo);
                    for (var i in attributeInfo) {
                        var attrInfo = attributeInfo[i];
                        preResourceData.ResourceAttributeInfo.push(attrInfo);
                        newAttributeInfo.push(attrInfo);
                    }
                });
                pptd.on('endTaskInfo', function () {
                    var filteredAttributeInfo = [];

                    preResourceData.ResourceAttributeInfo.forEach(function (attributeObj) {
                        if (filteredAttributeInfo.indexOf(attributeObj) === -1) {
                            filteredAttributeInfo.push(attributeObj);
                        }
                    });
                    //preResourceData.ResourceAttributeInfo = commonMethods.UniqueObjectArray(preResourceData.ResourceAttributeInfo, 'Attribute');;
                    preResourceData.ResourceAttributeInfo = filteredAttributeInfo;
                    callback(null, "", preResourceData, newAttributeInfo);
                });
            } else {
                callback(taskObj.Exception, taskObj.CustomMessage, preResourceData, newAttributeInfo);
            }
        }
    });
};

var SetConcurrencyInfo = function (data) {
    var e = new EventEmitter();
    process.nextTick(function () {
        if (Array.isArray(data) && data.length > 0) {
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
    if (data && data.length > 0) {
        var count = 0;
        for (var i in data) {
            redisHandler.GetObj(logKey, data[i], function (err, tempObj) {
                if (err) {
                    count++;
                    if (count === data.length) {
                        callback();
                    }
                } else {
                    var tagMetaKey = util.format('tagMeta:%s', data[i]);
                    redisHandler.GetObj(logKey, tagMetaKey, function (err, reTags) {
                        if (err) {
                            count++;
                            if (count === data.length) {
                                callback();
                            }
                        } else {
                            if (reTags) {
                                commonMethods.ConvertTagStrToArray(reTags, function (slotInfoTags) {
                                    var obj = JSON.parse(tempObj);
                                    //if (obj.ObjKey.search(/^(ConcurrencyInfo)[^\s]*/) != -1) {
                                    //    slotInfoTags = ["company_" + obj.Company, "tenant_" + obj.Tenant, "category_" + obj.Category, "resourceid_" + obj.ResourceId, "objtype_ConcurrencyInfo"];
                                    //}
                                    //else {
                                    //    slotInfoTags = ["company_" + obj.Company, "tenant_" + obj.Tenant, "category_" + obj.Category, "state_" + obj.State, "resourceid_" + obj.ResourceId, "objtype_CSlotInfo", "slotid_" + obj.SlotId];
                                    //}
                                    redisHandler.RemoveObj_V_T(logKey, obj.ObjKey, slotInfoTags, function (err, result) {
                                        count++;
                                        if (err) {
                                            console.log(err);
                                        }
                                        if (count === data.length) {
                                            callback();
                                        }
                                    });
                                });
                            } else {
                                count++;
                                if (count === data.length) {
                                    callback();
                                }
                            }
                        }
                    });
                }
            });
        }
    } else {
        callback();
    }
};

var RemoveResourceState = function (logKey, company, tenant, resourceid, callback) {
    var StateKey = util.format('ResourceState:%d:%d:%s', company, tenant, resourceid);
    redisHandler.RemoveObj(logKey, StateKey, function (err, result) {
        callback(err, result);
    });
};

var SetResourceLogin = function (logKey, basicData, callback) {
    var accessToken = util.format('%d:%d', basicData.Tenant, basicData.Company);
    var preResourceData = {};
    resourceService.GetResourceDetails(accessToken, basicData.ResourceId, function (resErr, resRes, resObj) {
        if (resErr) {
            callback(resErr, resRes, preResourceData);
        } else {
            if (resObj.IsSuccess) {
                var date = new Date();
                preResourceData = {
                    Company: resObj.Result.CompanyId,
                    Tenant: resObj.Result.TenantId,
                    Class: resObj.Result.ResClass,
                    Type: resObj.Result.ResType,
                    Category: resObj.Result.ResCategory,
                    ResourceId: resObj.Result.ResourceId.toString(),
                    ResourceName: resObj.Result.ResourceName,
                    UserName: basicData.UserName,
                    OtherInfo: resObj.Result.OtherData,
                    ConcurrencyInfo: [],
                    LoginTasks: [],
                    ResourceAttributeInfo: []
                };

                PreProcessResourceData(logKey, accessToken, preResourceData, basicData.HandlingTypes, function (err, msg, preProcessResData) {
                    if (err) {
                        callback(err, msg, null);
                    } else {
                        var concurrencyInfo = [];
                        var loginTasks = [];
                        var sci = SetConcurrencyInfo(preProcessResData.ConcurrencyInfo);

                        sci.on('concurrencyInfo', function (obj) {
                            //Validate login request with handling type
                            var validateHandlingType = commonMethods.FilterByID(basicData.HandlingTypes, "Type", obj.HandlingType);
                            //if(basicData.HandlingTypes.indexOf(obj.HandlingType) > -1 ) {
                            if (validateHandlingType) {
                                //var concurrencySlotInfo = [];
                                for (var i = 0; i < obj.NoOfSlots; i++) {
                                    var slotInfokey = util.format('CSlotInfo:%d:%d:%s:%s:%d', preProcessResData.Company, preProcessResData.Tenant, preProcessResData.ResourceId, obj.HandlingType, i);
                                    var slotInfo = {
                                        Company: preProcessResData.Company,
                                        Tenant: preProcessResData.Tenant,
                                        HandlingType: obj.HandlingType,
                                        State: "Available",
                                        StateChangeTime: date.toISOString(),
                                        HandlingRequest: "",
                                        LastReservedTime: "",
                                        MaxReservedTime: 10,
                                        MaxAfterWorkTime: 0,
                                        MaxFreezeTime: 0,
                                        FreezeAfterWorkTime: false,
                                        TempMaxRejectCount: 10,
                                        ResourceId: preProcessResData.ResourceId,
                                        SlotId: i,
                                        ObjKey: slotInfokey,
                                        OtherInfo: "",
                                        EnableToProductivity: obj.EnableToProductivity
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

                                var tempRefInfoObj = validateHandlingType.Contact;//JSON.parse(obj.RefInfo);
                                if (tempRefInfoObj) {
                                    tempRefInfoObj.ResourceId = preProcessResData.ResourceId;
                                    tempRefInfoObj.ResourceName = preProcessResData.ResourceName;
                                }
                                var tempRefInfoObjStr = JSON.stringify(tempRefInfoObj);
                                var concurrencyObj = {
                                    Company: preProcessResData.Company,
                                    Tenant: preProcessResData.Tenant,
                                    HandlingType: obj.HandlingType,
                                    LastConnectedTime: "",
                                    LastRejectedSession: "",
                                    RejectCount: 0,
                                    MaxRejectCount: 10,
                                    IsRejectCountExceeded: false,
                                    ResourceId: preProcessResData.ResourceId,
                                    UserName: basicData.UserName,
                                    ObjKey: cObjkey,
                                    RefInfo: tempRefInfoObjStr
                                };
                                var cObjTags = ["company_" + concurrencyObj.Company, "tenant_" + concurrencyObj.Tenant, "handlingType_" + concurrencyObj.HandlingType, "resourceid_" + preProcessResData.ResourceId, "objtype_ConcurrencyInfo"];
                                concurrencyInfo.push(cObjkey);
                                loginTasks.push(concurrencyObj.HandlingType);

                                var jsonConObj = JSON.stringify(concurrencyObj);
                                redisHandler.AddObj_V_T(logKey, cObjkey, jsonConObj, cObjTags, function (err, reply, vid) {
                                    if (err) {
                                        console.log(err);
                                    }
                                });
                            }
                        });

                        sci.on('endconcurrencyInfo', function () {
                            var resourceObj = {
                                Company: preProcessResData.Company,
                                Tenant: preProcessResData.Tenant,
                                Class: preProcessResData.Class,
                                Type: preProcessResData.Type,
                                Category: preProcessResData.Category,
                                ResourceId: preProcessResData.ResourceId,
                                ResourceName: preProcessResData.ResourceName,
                                UserName: basicData.UserName,
                                ResourceAttributeInfo: preProcessResData.ResourceAttributeInfo,
                                ConcurrencyInfo: concurrencyInfo,
                                LoginTasks: loginTasks,
                                OtherInfo: preProcessResData.OtherInfo
                            };

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
                                var resource_issMapKey = util.format('ResourceIssMap:%d:%d:%s', resourceObj.Company, resourceObj.Tenant, resourceObj.UserName);
                                redisHandler.SetObj_NX(logKey, resource_issMapKey, key, function () {
                                });
                                resourceStateMapper.SetResourceState(logKey, resourceObj.Company, resourceObj.Tenant, resourceObj.ResourceId, resourceObj.UserName, "Available", "Register", function (err, result) {
                                    if (resourceObj.ConcurrencyInfo && resourceObj.ConcurrencyInfo.length === 0) {
                                        resourceStateMapper.SetResourceState(logKey, resourceObj.Company, resourceObj.Tenant, resourceObj.ResourceId, resourceObj.UserName, "Available", "Offline", function (err, result) {
                                        });
                                    }
                                });
                                infoLogger.DetailLogger.log('info', '%s Finished AddResource. Result: %s', logKey, reply);
                                callback(err, reply, vid);
                            });
                        });
                    }
                });
            } else {
                callback(resObj.Exception, resObj.CustomMessage, preResourceData);
            }
        }
    });
};

var AddResource = function (logKey, basicData, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start AddResource *************************', logKey);
    var resourceKey = util.format('Resource:%d:%d:%s', basicData.Company, basicData.Tenant, basicData.ResourceId);
    redisHandler.CheckObjExists(logKey, resourceKey, function (err, isExists) {
        if (isExists == "1") {
            RemoveResource(logKey, basicData.Company.toString(), basicData.Tenant.toString(), basicData.ResourceId, function (err, result) {
                SetResourceLogin(logKey, basicData, function (err, reply, vid) {
                    callback(err, reply, vid);
                });
            });
        } else {
            SetResourceLogin(logKey, basicData, function (err, reply, vid) {
                callback(err, reply, vid);
            });
        }
    });
};

var EditResource = function (logKey, editType, accessToken, basicData, resourceData, callback) {
    var preResourceData = resourceData.Obj;
    var cVid = resourceData.Vid;
    var concurrencyInfo = deepcopy(preResourceData.ConcurrencyInfo);
    var loginTasks = deepcopy(preResourceData.LoginTasks);
    PreProcessResourceData(logKey, accessToken, preResourceData, basicData.HandlingTypes, function (err, msg, preProcessResData) {
        if (err) {
            callback(err, msg, null);
        } else {
            var date = new Date();
            var sci = SetConcurrencyInfo(preProcessResData.ConcurrencyInfo);

            sci.on('concurrencyInfo', function (obj) {
                //Validate login request with handling type
                var validateHandlingType = commonMethods.FilterByID(basicData.HandlingTypes, "Type", obj.HandlingType);
                //if(basicData.HandlingTypes.indexOf(obj.HandlingType) > -1 ) {
                if (validateHandlingType) {
                    //var concurrencySlotInfo = [];
                    var cObjkey = util.format('ConcurrencyInfo:%d:%d:%s:%s', preProcessResData.Company, preProcessResData.Tenant, preProcessResData.ResourceId, obj.HandlingType);
                    redisHandler.GetObj(logKey, cObjkey, function (cErr, isExists) {
                        if (cErr || !isExists) {
                            console.log(cErr);
                            isExists = undefined;
                        } else {
                            isExists = JSON.parse(isExists);
                        }

                        var tempRefInfoObj = validateHandlingType.Contact;//JSON.parse(obj.RefInfo);
                        if (tempRefInfoObj) {
                            tempRefInfoObj.ResourceId = preProcessResData.ResourceId;
                            tempRefInfoObj.ResourceName = preProcessResData.ResourceName;
                        }
                        var tempRefInfoObjStr = JSON.stringify(tempRefInfoObj);
                        var concurrencyObj = {
                            Company: preProcessResData.Company,
                            Tenant: preProcessResData.Tenant,
                            HandlingType: obj.HandlingType,
                            LastConnectedTime: "",
                            LastRejectedSession: "",
                            RejectCount: 0,
                            MaxRejectCount: 10,
                            IsRejectCountExceeded: false,
                            ResourceId: preProcessResData.ResourceId,
                            UserName: basicData.UserName,
                            ObjKey: cObjkey,
                            RefInfo: tempRefInfoObjStr
                        };
                        var cObjTags = ["company_" + basicData.Company, "tenant_" + basicData.Tenant, "handlingType_" + concurrencyObj.HandlingType, "resourceid_" + preProcessResData.ResourceId, "objtype_ConcurrencyInfo"];

                        var jsonConObj = JSON.stringify(concurrencyObj);
                        if (!isExists || editType == "addResource") {
                            if (concurrencyInfo.indexOf(cObjkey) === -1) {
                                loginTasks.push(concurrencyObj.HandlingType);
                                concurrencyInfo.push(cObjkey);
                            }
                            redisHandler.AddObj_V_T(logKey, cObjkey, jsonConObj, cObjTags, function (err, reply, vid) {
                                if (err) {
                                    console.log(err);
                                }
                            });
                        } else if (isExists && isExists.IsRejectCountExceeded == true) {
                            isExists.IsRejectCountExceeded = false;
                            isExists.RejectCount = 0;
                            redisHandler.SetObj_T(logKey, cObjkey, JSON.stringify(isExists), cObjTags, function (err, reply) {
                                if (err) {
                                    console.log(err);
                                }
                            });

                        } else {
                            var tagMetaKey = util.format('tagMeta:%s', cObjkey);
                            redisHandler.GetObj(logKey, tagMetaKey, function (err, ceTags) {
                                if (ceTags) {
                                    var newCompany = util.format('company_%s', basicData.Company);
                                    commonMethods.AppendNewCompanyTagStr(ceTags, newCompany, function (newTags) {
                                        redisHandler.SetTags(logKey, newTags, cObjkey, function (err, reply) {
                                            if (err) {
                                                console.log(err);
                                            }
                                        });
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
                                StateChangeTime: date.toISOString(),
                                HandlingRequest: "",
                                LastReservedTime: "",
                                MaxReservedTime: 10,
                                MaxAfterWorkTime: 0,
                                MaxFreezeTime: 0,
                                FreezeAfterWorkTime: false,
                                TempMaxRejectCount: 10,
                                ResourceId: preProcessResData.ResourceId,
                                SlotId: i,
                                ObjKey: slotInfokey,
                                OtherInfo: "",
                                EnableToProductivity: obj.EnableToProductivity
                            };
                            var slotInfoTags = ["company_" + basicData.Company, "tenant_" + basicData.Tenant, "handlingType_" + slotInfo.HandlingType, "state_" + slotInfo.State, "resourceid_" + preProcessResData.ResourceId, "slotid_" + i, "objtype_CSlotInfo"];

                            var jsonSlotObj = JSON.stringify(slotInfo);
                            if (!isExists || editType == "addResource") {
                                if (concurrencyInfo.indexOf(slotInfokey) === -1) {
                                    concurrencyInfo.push(slotInfokey);
                                }
                                redisHandler.AddObj_V_T(logKey, slotInfokey, jsonSlotObj, slotInfoTags, function (err, reply, vid) {
                                    if (err) {
                                        console.log(err);
                                    }
                                });
                            } else {
                                var slotTagMetaKey = util.format('tagMeta:%s', slotInfokey);
                                redisHandler.GetObj(logKey, slotTagMetaKey, function (err, seTags) {
                                    if (seTags) {
                                        var newCompany = util.format('company_%s', basicData.Company);
                                        commonMethods.AppendNewCompanyTagStr(seTags, newCompany, function (newsTags) {
                                            redisHandler.SetTags(logKey, newsTags, slotInfokey, function (err, reply) {
                                                if (err) {
                                                    console.log(err);
                                                }
                                            });
                                        });
                                    }
                                });
                            }
                        }
                    });
                }
            });

            sci.on('endconcurrencyInfo', function () {
                var resourceObj = {
                    Company: preProcessResData.Company,
                    Tenant: preProcessResData.Tenant,
                    Class: preProcessResData.Class,
                    Type: preProcessResData.Type,
                    Category: preProcessResData.Category,
                    ResourceId: preProcessResData.ResourceId,
                    ResourceName: preProcessResData.ResourceName,
                    UserName: basicData.UserName,
                    ResourceAttributeInfo: preProcessResData.ResourceAttributeInfo,
                    ConcurrencyInfo: concurrencyInfo,
                    LoginTasks: loginTasks,
                    OtherInfo: preProcessResData.OtherInfo
                };

                var key = util.format('Resource:%d:%d:%s', resourceObj.Company, resourceObj.Tenant, resourceObj.ResourceId);
                var tag = ["company_" + resourceObj.Company, "tenant_" + resourceObj.Tenant, "class_" + resourceObj.Class, "type_" + resourceObj.Type, "category_" + resourceObj.Category, "resourceid_" + resourceObj.ResourceId, "objtype_Resource"];

                var tagMetaKey = util.format('tagMeta:%s', key);
                redisHandler.GetObj(logKey, tagMetaKey, function (err, reTags) {
                    if (reTags) {
                        var newCompany = util.format('company_%s', basicData.Company);
                        commonMethods.AppendNewCompanyTagArray(reTags, newCompany, function (newTags) {
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
                                if (editType == "addResource") {
                                    var statusObj = {State: state, Reason: reason};
                                }
                                ardsMonitoringService.SendResourceStatus(accessToken, resourceObj.ResourceId, undefined);
                                callback(err, reply, vid);
                            });
                        });
                    }
                });
            });
        }
    });
};

var ShareResource = function (logKey, basicData, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start ShareResource *************************', logKey);
    var accessToken = util.format('%d:%d', basicData.Tenant, basicData.Company);
    var searchTag = ["resourceid_" + basicData.ResourceId, "objtype_Resource"];

    redisHandler.SearchObj_V_T(logKey, searchTag, function (err, strObj) {
        if (err) {
            console.log(err);
            callback(err, null, null);
        }
        else {
            if (strObj.length > 0) {
                EditResource(logKey, "shareResource", accessToken, basicData, strObj[0], function (err, reply, vid) {
                    callback(err, reply, vid);
                });
            } else {
                //callback(new Error("Resource Not Found"), null, null);
                SetResourceLogin(logKey, basicData, function (err, reply, vid) {
                    callback(err, reply, vid);
                });
            }
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
            if (resourceObj) {
                RemoveConcurrencyInfo(logKey, resourceObj.ConcurrencyInfo, function () {
                    //RemoveResourceState(logKey, resourceObj.Company, resourceObj.Tenant, resourceObj.ResourceId, function () {
                    //});

                    var tag = ["company_" + resourceObj.Company, "tenant_" + resourceObj.Tenant, "class_" + resourceObj.Class, "type_" + resourceObj.Type, "category_" + resourceObj.Category, "objtype_Resource", "resourceid_" + resourceObj.ResourceId];
                    var tagMetaKey = util.format('tagMeta:%s', key);
                    redisHandler.GetObj(logKey, tagMetaKey, function (err, reTags) {
                        if (reTags) {
                            var newCompany = util.format('company_%s', company);
                            commonMethods.AppendNewCompanyTagArray(reTags, newCompany, function (newTags) {
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
                    resourceStateMapper.SetResourceState(logKey, resourceObj.Company, resourceObj.Tenant, resourceObj.ResourceId, resourceObj.UserName, "NotAvailable", "UnRegister", function (err, result) {
                        redisHandler.RemoveObj_V_T(logKey, key, tag, function (err, result) {
                            if (err) {
                                infoLogger.DetailLogger.log('info', '%s Finished RemoveResource. Result: %s', logKey, "false");
                                callback(err, "false");
                            }
                            else {
                                var accessToken = util.format('%d:%d', resourceObj.Tenant, resourceObj.Company);
                                var pubAdditionalParams = util.format('resourceName=%s&statusType=%s', resourceObj.ResourceName, 'removeResource');
                                ardsMonitoringService.SendResourceStatus(accessToken, resourceObj.ResourceId, pubAdditionalParams);
                                infoLogger.DetailLogger.log('info', '%s Finished RemoveResource. Result: %s', logKey, result);
                                callback(null, result);
                            }
                        });
                    });

                });
            } else {
                callback(null, "true");
            }
        }
    });
};

var RemoveShareResource = function (logKey, company, tenant, resourceId, handlingType, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start RemoveShareResource *************************', logKey);
    var accessToken = util.format('%s:%s', tenant, company);
    var searchTag = ["resourceid_" + resourceId, "objtype_Resource"];

    redisHandler.SearchObj_V_T(logKey, searchTag, function (err, strObj) {
        if (err) {
            console.log(err);
            callback(err, null, null);
        }
        else {
            if (strObj && Array.isArray(strObj) && strObj.length > 0) {
                var preResourceData = strObj[0].Obj;
                var cVid = strObj[0].Vid;
                var concurrencyInfo = deepcopy(preResourceData.ConcurrencyInfo);
                var loginTasks = deepcopy(preResourceData.LoginTasks);
                var htArray = [{"Type": handlingType}];
                PreProcessResourceData(logKey, accessToken, preResourceData, htArray, function (err, msg, preProcessResData, attributeToRemove) {
                    if (err) {
                        callback(err, msg, null);
                    } else {
                        var sci = SetConcurrencyInfo(preProcessResData.ConcurrencyInfo);

                        sci.on('concurrencyInfo', function (obj) {
                            //Validate login request with handling type
                            if (obj.HandlingType == handlingType) {
                                //var concurrencySlotInfo = [];
                                var cObjkey = util.format('ConcurrencyInfo:%d:%d:%s:%s', preProcessResData.Company, preProcessResData.Tenant, preProcessResData.ResourceId, obj.HandlingType);
                                redisHandler.CheckObjExists(logKey, cObjkey, function (cErr, isExists) {
                                    if (cErr) {
                                        console.log(cErr);
                                        isExists = 0;
                                    }
                                    if (isExists == 1) {
                                        var tagMetaKey = util.format('tagMeta:%s', cObjkey);
                                        redisHandler.GetObj(logKey, tagMetaKey, function (err, ceTags) {
                                            if (ceTags) {
                                                var newCompany = util.format('company_%s', company);
                                                commonMethods.RemoveTagFromTagStr(ceTags, newCompany, function (newTags) {
                                                    if (newTags.includes("company_")) {
                                                        redisHandler.SetTags(logKey, newTags, cObjkey, function (err, reply) {
                                                            if (err) {
                                                                console.log(err);
                                                            }
                                                        });
                                                    } else {
                                                        commonMethods.ConvertTagStrToArray(newTags, function (slotInfoTags) {
                                                            redisHandler.RemoveObj_V_T(logKey, cObjkey, slotInfoTags, function (err, result) {
                                                                if (err) {
                                                                    console.log(err);
                                                                } else {
                                                                    console.log("Remove Concurrency Obj:: " + result);
                                                                }
                                                            });
                                                        });
                                                    }
                                                });
                                            }
                                        });
                                    }

                                    function setSlotDetail(slotInfokey, setSlotDetailCallback) {

                                        //var slotInfokey = util.format('CSlotInfo:%d:%d:%s:%s:%d', preProcessResData.Company, preProcessResData.Tenant, preProcessResData.ResourceId, obj.HandlingType, i);

                                        if (isExists == 1) {
                                            var slotTagMetaKey = util.format('tagMeta:%s', slotInfokey);
                                            redisHandler.GetObj(logKey, slotTagMetaKey, function (err, seTags) {
                                                if (seTags) {
                                                    var newCompany = util.format('company_%s', company);
                                                    commonMethods.RemoveTagFromTagStr(seTags, newCompany, function (newsTags) {
                                                        if (newsTags.includes("company_")) {
                                                            redisHandler.SetTags(logKey, newsTags, slotInfokey, function (err, reply) {
                                                                if (err) {
                                                                    console.log(err);
                                                                }
                                                            });
                                                        } else {
                                                            commonMethods.ConvertTagStrToArray(newsTags, function (slotInfoTags) {
                                                                redisHandler.RemoveObj_V_T(logKey, slotInfokey, slotInfoTags, function (err, result) {
                                                                    if (err) {
                                                                        console.log(err);
                                                                    } else {
                                                                        console.log("Remove ConcurrencySlot Obj:: " + result);
                                                                    }
                                                                });
                                                            });
                                                        }
                                                    });
                                                }
                                            });
                                        }

                                        setSlotDetailCallback();
                                    }

                                    var setSlotDetails = [];
                                    for (var i = 0; i < obj.NoOfSlots; i++) {
                                        var slotInfokey = util.format('CSlotInfo:%d:%d:%s:%s:%d', preProcessResData.Company, preProcessResData.Tenant, preProcessResData.ResourceId, obj.HandlingType, i);
                                        setSlotDetails.push(setSlotDetail.bind(this, slotInfokey));

                                    }

                                    async.parallel(setSlotDetails, function () {
                                        console.log("setSlotDetails Success");
                                    })
                                });
                            }
                        });

                        sci.on('endconcurrencyInfo', function () {

                            var resourceObj = {
                                Company: preProcessResData.Company,
                                Tenant: preProcessResData.Tenant,
                                Class: preProcessResData.Class,
                                Type: preProcessResData.Type,
                                Category: preProcessResData.Category,
                                ResourceId: preProcessResData.ResourceId,
                                ResourceName: preProcessResData.ResourceName,
                                UserName: preProcessResData.UserName,
                                ResourceAttributeInfo: preProcessResData.ResourceAttributeInfo,
                                ConcurrencyInfo: concurrencyInfo,
                                LoginTasks: loginTasks,
                                OtherInfo: preProcessResData.OtherInfo
                            };

                            var key = util.format('Resource:%d:%d:%s', resourceObj.Company, resourceObj.Tenant, resourceObj.ResourceId);

                            var tagMetaKey = util.format('tagMeta:%s', key);
                            redisHandler.GetObj(logKey, tagMetaKey, function (err, reTags) {
                                if (reTags) {
                                    var tagsToRemove = [];
                                    if (resourceObj.Company !== company) {
                                        tagsToRemove = [util.format('company_%s', company)];
                                    }

                                    var handlingTypesToRemove = [];
                                    if (attributeToRemove && attributeToRemove.length > 0) {
                                        for (var i in attributeToRemove) {
                                            if (handlingTypesToRemove.indexOf(attributeToRemove[i].HandlingType) === -1) {
                                                handlingTypesToRemove.push(attributeToRemove[i].HandlingType);
                                            }
                                            tagsToRemove.push("attribute_" + attributeToRemove[i].Attribute);
                                            resourceObj.ResourceAttributeInfo = commonMethods.RemoveItemFromObjectArray(resourceObj.ResourceAttributeInfo, 'Attribute', attributeToRemove[i].Attribute);
                                        }
                                    } else {
                                        handlingTypesToRemove.push(handlingType);
                                    }

                                    var cInfoCopy = deepcopy(concurrencyInfo);
                                    var tInfoCopy = deepcopy(loginTasks);

                                    for (var k in handlingTypesToRemove) {
                                        for (var j in cInfoCopy) {
                                            var cIndex = cInfoCopy[j].indexOf(handlingTypesToRemove[k]);
                                            if (cIndex > -1) {
                                                var removeIndex = resourceObj.ConcurrencyInfo.indexOf(cInfoCopy[j]);
                                                if (removeIndex > -1) {
                                                    resourceObj.ConcurrencyInfo.splice(removeIndex, 1);
                                                }
                                            }
                                        }
                                        //for(var l in tInfoCopy) {
                                        var tIndex = tInfoCopy.indexOf(handlingTypesToRemove[k]);
                                        if (tIndex > -1) {
                                            resourceObj.LoginTasks.splice(tIndex, 1);
                                        }
                                        //}
                                    }

                                    if (resourceObj.LoginTasks && resourceObj.LoginTasks.length === 0) {
                                        resourceStateMapper.SetResourceState(logKey, resourceObj.Company, resourceObj.Tenant, resourceObj.ResourceId, resourceObj.UserName, "Available", "Offline", function (err, result) {
                                        });
                                    } else {
                                        var pubAdditionalParams = util.format('resourceName=%s&statusType=%s&task=%s', resourceObj.ResourceName, 'removeTask', handlingType);
                                        ardsMonitoringService.SendResourceStatus(accessToken, resourceObj.ResourceId, pubAdditionalParams);

                                    }


                                    commonMethods.RemoveTagsFromTagArray(reTags, tagsToRemove, function (newTags) {
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
            } else {
                callback(new Error("No logged in Resource Found."), null, null);
            }
        }
    });
};

var SetResource = function (logKey, company, tenant, basicObj, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start SetResource *************************', logKey);

    var key = util.format('Resource:%d:%d:%s', company, tenant, basicObj.ResourceId);

    redisHandler.GetObj_V(logKey, key, function (err, jobj, cVid) {
        if (err) {
            console.log(err);
            callback(err, undefined, undefined);
        }
        else {
            if (jobj) {
                var obj = JSON.parse(jobj);
                var resourceObj = {
                    Company: obj.Company,
                    Tenant: obj.Tenant,
                    Class: obj.Class,
                    Type: obj.Type,
                    Category: obj.Category,
                    ResourceId: obj.ResourceId,
                    ResourceName: obj.ResourceName,
                    UserName: obj.UserName,
                    ResourceAttributeInfo: obj.ResourceAttributeInfo,
                    ConcurrencyInfo: obj.ConcurrencyInfo,
                    LoginTasks: obj.LoginTasks,
                    OtherInfo: basicObj.OtherInfo ? basicObj.OtherInfo : obj.OtherInfo
                };

                var checkExist = undefined;
                resourceObj.ResourceAttributeInfo.forEach(function (resAttInfo, resAttIndex) {

                    if (resAttInfo.Attribute === basicObj.ResourceAttributeInfo.Attribute) {
                        checkExist = resAttIndex;
                    }

                });

                //var checkExist = resourceObj.ResourceAttributeInfo.indexOf(basicObj.ResourceAttributeInfo);
                if (checkExist > -1) {
                    resourceObj.ResourceAttributeInfo[checkExist] = basicObj.ResourceAttributeInfo;
                } else {
                    //resourceObj.ResourceAttributeInfo[checkExist] = basicObj.ResourceAttributeInfo;

                    resourceObj.ResourceAttributeInfo.push(basicObj.ResourceAttributeInfo);
                }

                var defaultTags = ["company_" + resourceObj.Company, "tenant_" + resourceObj.Tenant, "class_" + resourceObj.Class, "type_" + resourceObj.Type, "category_" + resourceObj.Category, "resourceid_" + resourceObj.ResourceId, "objtype_Resource"];

                var tagMetaKey = util.format('tagMeta:%s', key);
                redisHandler.GetObj(logKey, tagMetaKey, function (err, reTags) {
                    if (reTags) {
                        var tagIndexToRemove = [];
                        var existingTags = reTags.split(":");

                        existingTags.splice(0, 1);

                        defaultTags = existingTags.filter(function (eTag) {
                            if (eTag.indexOf('attribute_') === -1) {
                                return eTag;
                            }
                        });


                    }


                    var tempAttributeList = [];
                    resourceObj.ResourceAttributeInfo.forEach(function (resAttrInfo) {
                        tempAttributeList.push(resAttrInfo.Attribute);
                    });

                    var sortedAttributes = sortArray.sortData(tempAttributeList);

                    sortedAttributes.forEach(function (sortedAttr) {
                        defaultTags.push("attribute_" + sortedAttr);
                    });

                    var jsonObj = JSON.stringify(resourceObj);

                    redisHandler.SetObj_V_T(logKey, key, jsonObj, defaultTags, cVid, function (err, reply, vid) {
                        infoLogger.DetailLogger.log('info', '%s Finished SetResource. Result: %s', logKey, reply);
                        callback(err, reply, vid);
                    });
                });
            } else {
                callback(new Error("No registerd resource found"), undefined, undefined);
            }

        }
    });
};

var GetResource = function (logKey, company, tenant, resourceId, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start GetResource *************************', logKey);

    var key = util.format('Resource:%s:%s:%s', company, tenant, resourceId);
    redisHandler.GetObj_V(logKey, key, function (err, result, vid) {
        infoLogger.DetailLogger.log('info', '%s Finished GetResource. Result: %s', logKey, result);
        if (err) {
            callback(err, undefined, 0);
        } else {
            if (result) {
                var resourceObj = JSON.parse(result);

                if (resourceObj.ConcurrencyInfo && resourceObj.ConcurrencyInfo.length > 0) {

                    redisHandler.MGetObj(logKey, resourceObj.ConcurrencyInfo, function (err, cObjs) {


                        if (cObjs && cObjs.length > 0) {

                            var tempConcurrencyInfo = [];
                            var tempSlotInfo = [];
                            for (var i = 0; i < cObjs.length; i++) {

                                var cObj = JSON.parse(cObjs[i]);

                                if (cObj.ObjKey.indexOf('CSlotInfo') > -1) {
                                    tempSlotInfo.push(cObj);
                                } else {
                                    cObj.SlotInfo = [];
                                    tempConcurrencyInfo.push(cObj);
                                }

                            }


                            for (var j = 0; j < tempConcurrencyInfo.length; j++) {
                                var tci = tempConcurrencyInfo[j];

                                for (var k = 0; k < tempSlotInfo.length; k++) {
                                    var rsi = tempSlotInfo[k];
                                    if (rsi.HandlingType === tci.HandlingType) {
                                        tci.SlotInfo.push(rsi);
                                    }
                                }
                            }


                            resourceObj.ConcurrencyInfo = tempConcurrencyInfo;

                        }

                        callback(err, resourceObj, vid);

                    });

                } else {

                    callback(err, resourceObj, vid);
                }

            } else {
                callback(undefined, undefined, vid);
            }
        }

    });
};

var GetResourceState = function (logKey, company, tenant, resourceId, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start GetResourceState *************************', logKey);

    var key = util.format('ResourceState:%s:%s:%s', company, tenant, resourceId);
    redisHandler.GetObj(logKey, key, function (err, result) {
        infoLogger.DetailLogger.log('info', '%s Finished GetResourceState. Result: %s', logKey, result);
        if (result) {
            callback(err, JSON.parse(result));
        } else {
            callback(new Error("No Resource State Found"), null);
        }
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

var UpdateLastConnectedTime = function (logKey, company, tenant, handlingType, resourceid, event, maxRejectCount, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start UpdateLastConnectedTime *************************', logKey);

    var cObjkey = util.format('ConcurrencyInfo:%d:%d:%s:%s', company, tenant, resourceid, handlingType);
    var date = new Date();

    redisHandler.GetObj_V(logKey, cObjkey, function (err, obj, vid) {
        if (err) {
            console.log(err);
        }
        else {
            var tagMetaKey = util.format('tagMeta:%s', cObjkey);
            redisHandler.GetObj(logKey, tagMetaKey, function (err, ceTags) {
                if (err) {
                    callback(err, null, null);
                }
                if (ceTags) {
                    commonMethods.GetSortedCompanyTagArray(ceTags, function (companyTags) {
                        var cObj = JSON.parse(obj);
                        if (event == "reserved") {
                            if (maxRejectCount)
                                cObj.MaxRejectCount = maxRejectCount;
                            cObj.LastConnectedTime = date.toISOString();
                        } else if (event == "connected") {
                            cObj.RejectCount = 0;
                        } else {
                            callback(new Error("Invalied event"), null, null);
                        }
                        var jCObj = JSON.stringify(cObj);
                        var tags = ["tenant_" + cObj.Tenant, "handlingType_" + cObj.HandlingType, "resourceid_" + cObj.ResourceId, "objtype_ConcurrencyInfo"];
                        var cObjTags = companyTags.concat(tags);
                        redisHandler.SetObj_V_T(logKey, cObjkey, jCObj, cObjTags, vid, function () {
                            infoLogger.DetailLogger.log('info', '%s Finished UpdateLastConnectedTime. Result: %s', logKey, result);
                            callback(err, result, vid);
                        });
                    });
                } else {
                    callback(new Error("Update Redis tags failed"), null, null);
                }
            });

        }
    });
};

var UpdateRejectCount = function (logKey, company, tenant, handlingType, resourceid, rejectedSession, reason, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start UpdateRejectCount *************************', logKey);

    var cObjkey = util.format('ConcurrencyInfo:%d:%d:%s:%s', company, tenant, resourceid, handlingType);
    var date = new Date();

    redisHandler.GetObj_V(logKey, cObjkey, function (err, obj, vid) {
        if (err) {
            console.log(err);
            callback(err, null, null);
        }
        else {
            var tagMetaKey = util.format('tagMeta:%s', cObjkey);
            redisHandler.GetObj(logKey, tagMetaKey, function (err, ceTags) {
                if (err) {
                    callback(err, null, null);
                }
                if (ceTags) {
                    commonMethods.GetSortedCompanyTagArray(ceTags, function (companyTags) {
                        var cObj = JSON.parse(obj);
                        cObj.RejectCount = cObj.RejectCount + 1;
                        cObj.LastRejectedSession = rejectedSession;
                        if (cObj.RejectCount >= cObj.MaxRejectCount) {
                            console.log('Reject Count Exceeded:: ' + cObj.UserName);
                            cObj.IsRejectCountExceeded = true;

                            var notificationMsg = {
                                From: "ARDS",
                                Direction: "STATELESS",
                                To: cObj.UserName,
                                Message: "Reject Count Exceeded!, Account Suspended for Task " + cObj.HandlingType
                            };
                            notificationService.SendNotificationInitiate(logKey, "message", "", notificationMsg, company, tenant);
                            notificationService.SendNotificationInitiate(logKey, "agent_suspended", "", notificationMsg, company, tenant);
                        }
                        var jCObj = JSON.stringify(cObj);
                        var tags = ["tenant_" + cObj.Tenant, "handlingType_" + cObj.HandlingType, "resourceid_" + cObj.ResourceId, "objtype_ConcurrencyInfo"];
                        var cObjTags = companyTags.concat(tags);

                        redisHandler.SetObj_V_T(logKey, cObjkey, jCObj, cObjTags, vid, function () {
                            infoLogger.DetailLogger.log('info', '%s Finished UpdateRejectCount. Result: %s', logKey, result);
                            callback(err, result, vid);
                        });

                        var internalToken = util.format("%d:%d", tenant, company);
                        resourceService.AddResourceTaskRejectInfo(internalToken, cObj.ResourceId, cObj.HandlingType, reason, "", rejectedSession, function () {
                        });
                    });
                } else {
                    callback(new Error("Update Redis tags failed"), null, null);
                }
            });
        }
    });
};

var UpdateSlotStateAvailable = function (logKey, company, tenant, handlingType, resourceid, slotid, reason, otherInfo, callingParty, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start UpdateSlotStateAvailable *************************', logKey);

    var slotInfokey = util.format('CSlotInfo:%s:%s:%s:%s:%s', company, tenant, resourceid, handlingType, slotid);
    redisHandler.GetObj_V(logKey, slotInfokey, function (err, obj, vid) {
        if (err) {
            console.log(err);
            callback(err, false);
        }
        else {
            var tempObj = JSON.parse(obj);
            var tempObjCopy = deepcopy(tempObj);
            console.log("==========================================================================================");
            console.log("callingParty:: " + callingParty);
            console.log("tempObj.State:: " + tempObj.State);

            if (callingParty === "Completed" && tempObj.State === "AfterWork" && tempObj.FreezeAfterWorkTime === true) {
                console.log("Reject Available Request:: Completed");
                callback(new Error("Resource in AfterWork Freeze State"), null);
            } else if (callingParty === "Completed" && tempObj.State === "Reserved") {
                console.log("Reject Available Request:: Reserved");
                callback(new Error("Resource in Reserved State"), null);
            } else if (callingParty === "Completed" && tempObj.State === "Connected") {
                console.log("Reject Available Request:: Connected");
                callback(new Error("Resource in Connected State"), null);
            } else if (callingParty === "endFreeze" && tempObj.State != "AfterWork") {
                console.log("Reject Available Request:: endFreeze");
                callback(new Error("Resource in Connected State"), null);
            } else {
                console.log("Set Available");
                var tagMetaKey = util.format('tagMeta:%s', slotInfokey);
                redisHandler.GetObj(logKey, tagMetaKey, function (err, ceTags) {
                    if (err) {
                        callback(err, null, null);
                    }
                    if (ceTags) {
                        commonMethods.GetSortedCompanyTagArray(ceTags, function (companyTags) {
                            var date = new Date();
                            var handledRequest = tempObj.HandlingRequest;

                            if (tempObj.FreezeAfterWorkTime) {
                                try {
                                    scheduleWorkerHandler.endFreeze(company, tenant, resourceid, logKey);
                                } catch (ex) {
                                    console.log('scheduleWorkerHandler.endFreeze Error :: ' + ex);
                                }
                            }
                            tempObj.State = "Available";
                            tempObj.StateChangeTime = date.toISOString();
                            tempObj.HandlingRequest = "";
                            tempObj.FreezeAfterWorkTime = false;
                            tempObj.OtherInfo = "";
                            var tags = ["tenant_" + tempObj.Tenant, "handlingType_" + tempObj.HandlingType, "state_" + tempObj.State, "resourceid_" + tempObj.ResourceId, "slotid_" + tempObj.SlotId, "objtype_CSlotInfo"];
                            var slotInfoTags = companyTags.concat(tags);
                            var jsonObj = JSON.stringify(tempObj);
                            redisHandler.SetObj_V_T(logKey, slotInfokey, jsonObj, slotInfoTags, vid, function (err, reply, vid) {
                                infoLogger.DetailLogger.log('info', '%s Finished UpdateSlotStateAvailable. Result: %s', logKey, reply);
                                if (err != null) {
                                    console.log(err);
                                }
                                else {
                                    var duration = moment(tempObj.StateChangeTime).diff(moment(tempObjCopy.StateChangeTime), 'seconds');
                                    var internalAccessToken = util.format('%s:%s', tenant, company);
                                    resourceService.AddResourceStatusChangeInfo(internalAccessToken, tempObj.ResourceId, "SloatStatus", tempObj.State, otherInfo, {
                                        SessionId: handledRequest,
                                        Direction: ""
                                    }, function (err, result, obj) {
                                        if (err) {
                                            console.log("AddResourceStatusChangeInfo Failed.", err);
                                        } else {
                                            console.log("AddResourceStatusChangeInfo Success.", obj);
                                            resourceService.AddResourceStatusDurationInfo(internalAccessToken, tempObj.ResourceId, "SloatStatus", tempObjCopy.State, "", tempObjCopy.OtherInfo, tempObjCopy.HandlingRequest, duration, function () {
                                                if (err) {
                                                    console.log("AddResourceStatusDurationInfo Failed.", err);
                                                } else {
                                                    console.log("AddResourceStatusDurationInfo Success.", obj);
                                                }
                                            });
                                        }
                                    });
                                }
                                callback(err, reply);
                            });
                        });
                    } else {
                        callback(new Error("Update Redis tags failed"), null, null);
                    }
                });
            }
        }
    });
};

var UpdateSlotStateAfterWork = function (logKey, company, tenant, handlingType, resourceid, slotid, sessionid, reason, otherInfo, direction, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start UpdateSlotStateAfterWork *************************', logKey);

    var slotInfokey = util.format('CSlotInfo:%s:%s:%s:%s:%s', company, tenant, resourceid, handlingType, slotid);
    redisHandler.GetObj_V(logKey, slotInfokey, function (err, obj, vid) {
        if (err) {
            console.log(err);
            callback(err, false);
        }
        else {
            var tagMetaKey = util.format('tagMeta:%s', slotInfokey);
            redisHandler.GetObj(logKey, tagMetaKey, function (err, ceTags) {
                if (err) {
                    callback(err, null);
                }
                if (ceTags) {
                    commonMethods.GetSortedCompanyTagArray(ceTags, function (companyTags) {
                        var date = new Date();
                        var tempObj = JSON.parse(obj);
                        var tempObjCopy = deepcopy(tempObj);
                        if (tempObj.State === "Connected" || tempObj.State === "Reserved") {
                            var handledRequest = tempObj.HandlingRequest;

                            tempObj.State = "AfterWork";
                            tempObj.StateChangeTime = date.toISOString();
                            tempObj.OtherInfo = "";
                            var tags = ["tenant_" + tempObj.Tenant, "handlingType_" + tempObj.HandlingType, "state_" + tempObj.State, "resourceid_" + tempObj.ResourceId, "slotid_" + tempObj.SlotId, "handlingrequest_" + tempObj.HandlingRequest, "objtype_CSlotInfo"];
                            var slotInfoTags = companyTags.concat(tags);
                            var jsonObj = JSON.stringify(tempObj);
                            redisHandler.SetObj_V_T(logKey, slotInfokey, jsonObj, slotInfoTags, vid, function (err, reply, vid) {
                                infoLogger.DetailLogger.log('info', '%s Finished UpdateSlotStateAfterWork. Result: %s', logKey, reply);
                                if (err != null) {
                                    console.log(err);
                                }
                                else {
                                    var duration = moment(tempObj.StateChangeTime).diff(moment(tempObjCopy.StateChangeTime), 'seconds');
                                    SetProductivityData(logKey, company, tenant, resourceid, "Completed");
                                    var internalAccessToken = util.format('%s:%s', tenant, company);
                                    resourceService.AddResourceStatusChangeInfo(internalAccessToken, resourceid, "SloatStatus", "Completed", handlingType, {
                                        SessionId: sessionid,
                                        Direction: handlingType + direction
                                    }, function (err, result, obj) {
                                        if (err) {
                                            console.log("AddResourceStatusChangeInfo Failed.", err);
                                        } else {
                                            console.log("AddResourceStatusChangeInfo Success.", obj);
                                            resourceService.AddResourceStatusDurationInfo(internalAccessToken, tempObj.ResourceId, "SloatStatus", tempObjCopy.State, "", tempObjCopy.OtherInfo, tempObjCopy.HandlingRequest, duration, function () {
                                                if (err) {
                                                    console.log("AddResourceStatusDurationInfo Failed.", err);
                                                } else {
                                                    console.log("AddResourceStatusDurationInfo Success.", obj);
                                                }
                                            });
                                        }

                                        resourceService.AddResourceStatusChangeInfo(internalAccessToken, resourceid, "SloatStatus", "Completed", "AfterWork", {
                                            SessionId: sessionid,
                                            Direction: handlingType + direction
                                        }, function (err, result, obj) {
                                            if (err) {
                                                console.log("AddResourceStatusChangeInfo Failed.", err);
                                            } else {
                                                console.log("AddResourceStatusChangeInfo Success.", obj);
                                            }
                                        });
                                    });
                                }
                                callback(err, reply);
                            });
                        }
                    });
                } else {
                    callback(new Error("Update Redis tags failed"), null);
                }
            });
        }
    });
};

var UpdateSlotStateReserved = function (logKey, company, tenant, handlingType, resourceid, slotid, sessionid, maxReservedTime, maxAfterWorkTime, maxFreezeTime, maxRejectCount, otherInfo, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start UpdateSlotStateReserved *************************', logKey);

    var slotInfokey = util.format('CSlotInfo:%s:%s:%s:%s:%s', company, tenant, resourceid, handlingType, slotid);
    redisHandler.GetObj_V(logKey, slotInfokey, function (err, obj, vid) {
        if (err) {
            console.log(err);
            callback(err, false);
        }
        else {
            var tagMetaKey = util.format('tagMeta:%s', slotInfokey);
            redisHandler.GetObj(logKey, tagMetaKey, function (err, ceTags) {
                if (err) {
                    callback(err, null, null);
                }
                if (ceTags) {
                    commonMethods.GetSortedCompanyTagArray(ceTags, function (companyTags) {
                        var date = new Date();
                        var tempObj = JSON.parse(obj);
                        var tempObjCopy = deepcopy(tempObj);
                        tempObj.State = "Reserved";
                        tempObj.StateChangeTime = date.toISOString();
                        tempObj.HandlingRequest = sessionid;
                        tempObj.LastReservedTime = date.toISOString();
                        tempObj.OtherInfo = otherInfo;
                        if (maxReservedTime)
                            tempObj.MaxReservedTime = maxReservedTime;
                        if (maxAfterWorkTime)
                            tempObj.MaxAfterWorkTime = maxAfterWorkTime;
                        if (maxFreezeTime)
                            tempObj.MaxFreezeTime = maxFreezeTime;
                        var tags = ["tenant_" + tempObj.Tenant, "handlingType_" + tempObj.HandlingType, "state_" + tempObj.State, "resourceid_" + tempObj.ResourceId, "slotid_" + tempObj.SlotId, "handlingrequest_" + tempObj.HandlingRequest, "objtype_CSlotInfo"];
                        var slotInfoTags = companyTags.concat(tags);
                        var jsonObj = JSON.stringify(tempObj);
                        redisHandler.SetObj_V_T(logKey, slotInfokey, jsonObj, slotInfoTags, vid, function (err, reply, vid) {
                            infoLogger.DetailLogger.log('info', '%s Finished UpdateSlotStateReserved. Result: %s', logKey, reply);
                            if (err != null) {
                                console.log(err);
                            }
                            else {
                                UpdateLastConnectedTime(logKey, tempObj.Company, tempObj.Tenant, tempObj.HandlingType, resourceid, "reserved", maxRejectCount, function () {
                                });

                                var duration = moment(tempObj.StateChangeTime).diff(moment(tempObjCopy.StateChangeTime), 'seconds');
                                var internalAccessToken = util.format('%s:%s', tenant, company);
                                resourceService.AddResourceStatusChangeInfo(internalAccessToken, tempObj.ResourceId, "SloatStatus", tempObj.State, otherInfo, {
                                    SessionId: sessionid,
                                    Direction: ""
                                }, function (err, result, obj) {
                                    if (err) {
                                        console.log("AddResourceStatusChangeInfo Failed.", err);
                                    } else {
                                        console.log("AddResourceStatusChangeInfo Success.", obj);
                                        resourceService.AddResourceStatusDurationInfo(internalAccessToken, tempObj.ResourceId, "SloatStatus", tempObjCopy.State, "", tempObjCopy.OtherInfo, tempObjCopy.HandlingRequest, duration, function () {
                                            if (err) {
                                                console.log("AddResourceStatusDurationInfo Failed.", err);
                                            } else {
                                                console.log("AddResourceStatusDurationInfo Success.", obj);
                                            }
                                        });
                                    }
                                });
                            }
                            callback(err, reply);
                        });
                    });
                } else {
                    callback(new Error("Update Redis tags failed"), null, null);
                }
            });
        }
    });
};

var UpdateSlotStateConnected = function (logKey, company, tenant, handlingType, resourceid, slotid, sessionid, otherInfo, direction, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start UpdateSlotStateConnected *************************', logKey);

    var slotInfokey = util.format('CSlotInfo:%s:%s:%s:%s:%s', company, tenant, resourceid, handlingType, slotid);
    redisHandler.GetObj_V(logKey, slotInfokey, function (err, obj, vid) {
        if (err) {
            console.log(err);
            callback(err, false);
        }
        else {
            var tagMetaKey = util.format('tagMeta:%s', slotInfokey);
            redisHandler.GetObj(logKey, tagMetaKey, function (err, ceTags) {
                if (err) {
                    callback(err, null, null);
                }
                if (ceTags) {
                    commonMethods.GetSortedCompanyTagArray(ceTags, function (companyTags) {
                        var date = new Date();
                        var tempObj = JSON.parse(obj);
                        var tempObjCopy = deepcopy(tempObj);
                        tempObj.State = "Connected";
                        tempObj.StateChangeTime = date.toISOString();
                        tempObj.HandlingRequest = sessionid;
                        tempObj.OtherInfo = otherInfo;
                        var tags = ["tenant_" + tempObj.Tenant, "handlingType_" + tempObj.HandlingType, "state_" + tempObj.State, "resourceid_" + tempObj.ResourceId, "slotid_" + tempObj.SlotId, "handlingrequest_" + tempObj.HandlingRequest, "objtype_CSlotInfo"];
                        var slotInfoTags = companyTags.concat(tags);
                        var jsonObj = JSON.stringify(tempObj);
                        redisHandler.SetObj_V_T(logKey, slotInfokey, jsonObj, slotInfoTags, vid, function (err, reply, vid) {
                            infoLogger.DetailLogger.log('info', '%s Finished UpdateSlotStateConnected. Result: %s', logKey, reply);
                            if (err != null) {
                                console.log(err);
                            }
                            else {
                                UpdateLastConnectedTime(logKey, tempObj.Company, tempObj.Tenant, tempObj.HandlingType, resourceid, "connected", function () {
                                });
                                SetProductivityData(logKey, company, tenant, resourceid, "Connected");
                                var internalAccessToken = util.format('%s:%s', tenant, company);
                                var duration = moment(tempObj.StateChangeTime).diff(moment(tempObjCopy.StateChangeTime), 'seconds');
                                if (otherInfo == "" || otherInfo == null) {
                                    otherInfo = "Connected";
                                }
                                resourceService.AddResourceStatusChangeInfo(internalAccessToken, tempObj.ResourceId, "SloatStatus", tempObj.State, handlingType, {
                                    SessionId: sessionid,
                                    Direction: direction
                                }, function (err, result, obj) {
                                    if (err) {
                                        console.log("AddResourceStatusChangeInfo Failed.", err);
                                    } else {
                                        console.log("AddResourceStatusChangeInfo Success.", obj);
                                        resourceService.AddResourceStatusDurationInfo(internalAccessToken, tempObj.ResourceId, "SloatStatus", tempObjCopy.State, "", tempObjCopy.OtherInfo, tempObjCopy.HandlingRequest, duration, function () {
                                            if (err) {
                                                console.log("AddResourceStatusDurationInfo Failed.", err);
                                            } else {
                                                console.log("AddResourceStatusDurationInfo Success.", obj);
                                            }
                                        });
                                    }
                                });
                            }
                            callback(err, reply);
                        });
                    });
                } else {
                    callback(new Error("Update Redis tags failed"), null, null);
                }
            });
        }
    });
};

var UpdateSlotStateCompleted = function (logKey, company, tenant, handlingType, resourceid, slotid, sessionid, otherInfo, direction, callback) {
    UpdateSlotStateAfterWork(logKey, company, tenant, handlingType, resourceid, slotid, sessionid, "", "", direction, function (err, reply) {
        console.log(reply);
    });
    var slotInfokey = util.format('CSlotInfo:%s:%s:%s:%s:%s', company, tenant, resourceid, handlingType, slotid);
    console.log("AfterWorkStart: " + Date.now());
    redisHandler.GetObj(logKey, slotInfokey, function (err, slotObjStr) {
        console.log("GetObjSuccess: " + slotInfokey);
        var timeOut = 10000;
        if (err) {
            setTimeout(function () {
                console.log("AfterWorkEnd: " + Date.now());
                UpdateSlotStateAvailable(logKey, company, tenant, handlingType, resourceid, slotid, "", "AfterWork", "Completed", function (err, result) {
                });
            }, timeOut);
        } else {
            if (slotObjStr) {
                var slotObj = JSON.parse(slotObjStr);
                console.log("MaxAfterWorkTime: " + slotObj.MaxAfterWorkTime);
                var awTime = 10;
                if (slotObj.MaxAfterWorkTime) {
                    awTime = parseInt(slotObj.MaxAfterWorkTime);
                    console.log("MaxAfterWorkTime_converte: " + awTime);
                    if (awTime === 0) {
                        awTime = 10;
                    }
                }
                timeOut = awTime * 1000;
                console.log("New timeout: " + timeOut);
                setTimeout(function () {
                    console.log("AfterWorkEnd: " + Date.now());
                    UpdateSlotStateAvailable(logKey, company, tenant, handlingType, resourceid, slotid, "", "AfterWork", "Completed", function (err, result) {
                    });
                }, timeOut);
            } else {
                setTimeout(function () {
                    console.log("AfterWorkEnd: " + Date.now());
                    UpdateSlotStateAvailable(logKey, company, tenant, handlingType, resourceid, slotid, "", "AfterWork", "Completed", function (err, result) {
                    });
                }, timeOut);
            }
        }
    });
    callback(null, "OK");
};

var SetSlotStateFreeze = function (logKey, company, tenant, handlingType, resourceid, slotid, reason, otherInfo, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start SetSlotStateFreeze *************************', logKey);

    var slotInfokey = util.format('CSlotInfo:%s:%s:%s:%s:%s', company, tenant, resourceid, handlingType, slotid);
    redisHandler.GetObj_V(logKey, slotInfokey, function (err, obj, vid) {
        if (err) {
            console.log(err);
            callback(err, false);
        }
        else {
            var tempObj = JSON.parse(obj);
            if (tempObj.State === "AfterWork") {
                var tagMetaKey = util.format('tagMeta:%s', slotInfokey);
                redisHandler.GetObj(logKey, tagMetaKey, function (err, ceTags) {
                    if (err) {
                        callback(err, null, null);
                    }
                    if (ceTags) {
                        commonMethods.GetSortedCompanyTagArray(ceTags, function (companyTags) {

                            tempObj.FreezeAfterWorkTime = true;

                            var tags = ["tenant_" + tempObj.Tenant, "handlingType_" + tempObj.HandlingType, "state_" + tempObj.State, "resourceid_" + tempObj.ResourceId, "slotid_" + tempObj.SlotId, "handlingrequest_" + tempObj.HandlingRequest, "objtype_CSlotInfo"];
                            var slotInfoTags = companyTags.concat(tags);
                            var jsonObj = JSON.stringify(tempObj);
                            redisHandler.SetObj_V_T(logKey, slotInfokey, jsonObj, slotInfoTags, vid, function (err, reply, vid) {
                                infoLogger.DetailLogger.log('info', '%s Finished SetSlotStateFreeze. Result: %s', logKey, reply);
                                if (err != null) {
                                    console.log(err);
                                }
                                try {
                                    scheduleWorkerHandler.startFreeze(company, tenant, resourceid, resourceid, tempObj.MaxFreezeTime, tempObj.HandlingRequest, logKey);
                                }
                                catch (ex) {
                                    console.log('scheduleWorkerHandler.startFreeze :: ' + ex);
                                }
                                callback(err, reply);
                            });

                        });
                    } else {
                        callback(new Error("Update Redis tags failed"), null, null);
                    }
                });

            } else {
                callback(new Error("Cannot Freeze, Resource not in AfterWork State"), null);
            }
        }
    });
};


var UpdateSlotStateBySessionId = function (logKey, company, tenant, handlingType, resourceid, sessionid, state, reason, otherInfo, direction, callback) {
    var slotInfoTags = [];

    if (direction === "outbound" && (state.toLowerCase() === "reserved" ||state.toLowerCase() === "connected")) {

        slotInfoTags = ["company_" + company, "tenant_" + tenant, "handlingType_" + handlingType, "resourceid_" + resourceid];

        SearchCSlotByTags(logKey, slotInfoTags, function (err, cslots) {
            if (err) {
                console.log(err);
                callback(err, null);
            }
            else {
                if (cslots != null && cslots.length > 0) {
                    var selectedSlot = undefined;

                    if (cslots.length === 1) {
                        selectedSlot = cslots[0].Obj;
                    } else {
                        var reservedSlots = [];
                        var afterWorkSlots = [];
                        var connectedSlots = [];
                        for (var i = 0; i < cslots.length; i++) {
                            var cs = cslots[i].Obj;
                            if (cs.State === "Available") {
                                selectedSlot = cs;
                                break;
                            } else {
                                switch (cs.State) {
                                    case "Reserved":
                                        reservedSlots.push(cs);
                                        break;
                                    case "AfterWork":
                                        afterWorkSlots.push(cs);
                                        break;
                                    // case "Connected":
                                    //     connectedSlots.push(cs);
                                    //     break;
                                    default :
                                        break;
                                }
                            }
                        }

                        if (!selectedSlot) {
                            if (afterWorkSlots.length > 0) {
                                selectedSlot = afterWorkSlots[0];
                            } else if (reservedSlots.length > 0) {
                                selectedSlot = reservedSlots[0];
                            } else if (connectedSlots.length > 0) {
                                selectedSlot = connectedSlots[0];
                            }
                        }
                    }


                    if (selectedSlot) {
                        if(state.toLowerCase() === "connected") {
                            UpdateSlotStateConnected(logKey, selectedSlot.Company, selectedSlot.Tenant, selectedSlot.HandlingType, selectedSlot.ResourceId, selectedSlot.SlotId, sessionid, otherInfo, direction, function (err, result) {
                                callback(err, result);
                            });
                        }else if(state.toLowerCase() === "reserved" && selectedSlot.State !== "Connected") {
                            UpdateSlotStateReserved(logKey, selectedSlot.Company, selectedSlot.Tenant, selectedSlot.HandlingType, selectedSlot.ResourceId, selectedSlot.SlotId, sessionid, null, null, null, null, otherInfo, function (err, result) {
                                callback(err, result);
                            });
                        }else {
                            callback(new Error("Invalid outbound status change request"), undefined);
                        }
                    } else {
                        callback(new Error("No Resource CSlot found"), undefined);
                    }

                }
                else {
                    callback(new Error("No Resource CSlot found"), undefined);
                }
            }
        });
    } else {
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
                if (state == "Reject") {
                    UpdateRejectCount(logKey, company, tenant, handlingType, resourceid, sessionid, reason, function (err, result, vid) {
                        //callback(err, result);
                    });
                    var pubMessage = util.format("EVENT:%s:%s:%s:%s:%s:%s:%s:%s:YYYY", tenant, company, "ARDS", "REQUEST", "REJECT", reason, resourceid, sessionid);
                    redisHandler.Publish(logKey, "events", pubMessage, function () {
                    });
                }
                if (cslots && cslots.length > 0) {
                    for (var i in cslots) {
                        var cs = cslots[i].Obj;
                        if (cs.HandlingRequest == sessionid) {
                            switch (state) {
                                case "Reject":
                                    UpdateSlotStateCompleted(logKey, cs.Company, cs.Tenant, cs.HandlingType, cs.ResourceId, cs.SlotId, sessionid, otherInfo, direction, function (err, result) {
                                        callback(err, result);
                                    });
                                    break;

                                case "Available":
                                    UpdateSlotStateAvailable(logKey, cs.Company, cs.Tenant, cs.HandlingType, cs.ResourceId, cs.SlotId, reason, otherInfo, "Available", function (err, result) {
                                        callback(err, result);
                                    });
                                    break;

                                case "Connected":
                                    UpdateSlotStateConnected(logKey, cs.Company, cs.Tenant, cs.HandlingType, cs.ResourceId, cs.SlotId, sessionid, otherInfo, direction, function (err, result) {
                                        callback(err, result);
                                    });
                                    break;

                                case "Completed":
                                    UpdateSlotStateCompleted(logKey, cs.Company, cs.Tenant, cs.HandlingType, cs.ResourceId, cs.SlotId, sessionid, otherInfo, direction, function (err, result) {
                                        callback(err, result);
                                    });
                                    break;

                                case "Freeze":
                                    SetSlotStateFreeze(logKey, cs.Company, cs.Tenant, cs.HandlingType, cs.ResourceId, cs.SlotId, reason, otherInfo, function (err, result) {
                                        callback(err, result);
                                    });
                                    break;

                                case "endFreeze":
                                    UpdateSlotStateAvailable(logKey, cs.Company, cs.Tenant, cs.HandlingType, cs.ResourceId, cs.SlotId, "", "AfterWork", "endFreeze", function (err, result) {
                                        callback(err, result);
                                    });
                                    break;

                                default :
                                    callback(err, "Invalid Request");
                            }
                            break;
                        }
                    }
                }
                else {
                    callback(new Error("No Reserved Resource CSlot found for sessionId: " + sessionid), undefined);
                }
            }
        });
    }
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

var DoResourceSelection = function (company, tenant, resourceCount, sessionId, serverType, requestType, selectionAlgo, handlingAlgo, otherInfo, callback) {


    var rUrl = util.format('http://%s', config.Services.routingServiceHost)

    if (validator.isIP(config.Services.routingServiceHost)) {
        rUrl = util.format('http://%s:%s', config.Services.routingServiceHost, config.Services.routingServicePort)
    }
    var params = util.format('/resourceselection/getresource/%d/%d/%d/%s/%s/%s/%s/%s/%s', company, tenant, resourceCount, sessionId, serverType, requestType, selectionAlgo, handlingAlgo, otherInfo);
    restClientHandler.PickResource(rUrl, params, function (err, res, obj) {
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