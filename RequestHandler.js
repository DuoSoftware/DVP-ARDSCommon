﻿var util = require('util');
var redisHandler = require('./RedisHandler.js');
var dashboardEventHandler = require('./DashboardEventHandler');
var sortArray = require('./CommonMethods.js');
var reqQueueHandler = require('./ReqQueueHandler.js');
var resourceHandler = require('./ResourceHandler.js');
var logger = require("dvp-common/LogHandler/CommonLogHandler.js").logger;

var AddRequest = function (logKey, requestObj, callback) {
    logger.info('%s ************************* Start AddRequest *************************', logKey);

    var key = util.format('Request:%d:%d:%s', requestObj.Company, requestObj.Tenant, requestObj.SessionId);
    var tag = ["company_" + requestObj.Company, "tenant_" + requestObj.Tenant, "serverType_" + requestObj.ServerType, "requestType_" + requestObj.RequestType, "sessionid_" + requestObj.SessionId, "reqserverid_" + requestObj.RequestServerId, "priority_" + requestObj.Priority, "servingalgo_" + requestObj.ServingAlgo, "handlingalgo_" + requestObj.HandlingAlgo, "selectionalgo_" + requestObj.SelectionAlgo, "objtype_Request"];

    var tempAttributeList = [];
    for (var i in requestObj.AttributeInfo) {
        var atts = requestObj.AttributeInfo[i].AttributeCode;
        for (var j in atts) {
            tempAttributeList.push(atts[j]);
        }
    }
    var sortedAttributes = sortArray.sortData(tempAttributeList);
    for (var k in sortedAttributes) {
        tag.push("attribute_" + sortedAttributes[k]);
    }

    if (requestObj.ResourceCount == null){
        requestObj.ResourceCount = 1;
    }

    var jsonObj = JSON.stringify(requestObj);

    redisHandler.AddObj_V_T(key, jsonObj, tag, function (err, reply, vid) {
        if (err) {
            logger.error(err);
        }
        SetRequestState(requestObj.Company, requestObj.Tenant, requestObj.SessionId, "N/A", function (err, result) {
            //var pubMessage = util.format("EVENT:%d:%d:%s:%s:%s:%s:%s:%s:YYYY", requestObj.Tenant, requestObj.Company, "ARDS", "REQUEST", "ADDED", "", "", requestObj.SessionId);

            var eventTime = new Date().toISOString();
            dashboardEventHandler.PublishEvent(logKey, requestObj.Tenant, requestObj.Company, requestObj.BusinessUnit, "ARDS", "REQUEST", "ADDED", "", "", requestObj.SessionId, eventTime);
        });
        callback(err, reply, vid);
    });
};

var SetRequest = function (logKey, requestObj, cVid, callback) {
    logger.info('%s ************************* Start SetRequest *************************', logKey);

    var key = util.format('Request:%d:%d:%s', requestObj.Company, requestObj.Tenant, requestObj.SessionId);
    redisHandler.CheckObjExists(logKey, key, function (err, result) {
        if (err) {
            logger.error(err);
            callback(err, null, 0);
        }
        else if (result == "1") {
            var tag = ["company_" + requestObj.Company, "tenant_" + requestObj.Tenant, "serverType_" + requestObj.ServerType, "requestType_" + requestObj.RequestType, "objtype_Request", "sessionid_" + requestObj.SessionId, "reqserverid_" + requestObj.RequestServerId, "priority_" + requestObj.Priority, "servingalgo_" + requestObj.ServingAlgo, "handlingalgo" + requestObj.HandlingAlgo, "selectionalgo" + requestObj.SelectionAlgo];
            var tempAttributeList = [];
            for (var i in requestObj.AttributeInfo) {
                var atts = requestObj.AttributeInfo[i].AttributeCode;
                for (var j in atts) {
                    tempAttributeList.push(atts[j]);
                }
            }
            var sortedAttributes = sortArray.sortData(tempAttributeList);
            for (var k in sortedAttributes) {
                tag.push("attribute_" + sortedAttributes[k]);
            }

            if (requestObj.ResourceCount == null){
                requestObj.ResourceCount = 1;
            }
            var jsonObj = JSON.stringify(requestObj);

            redisHandler.SetObj_V_T(logKey, key, jsonObj, tag, cVid, function (err, reply, vid) {
                logger.info('%s Finished SetRequest. Result: %s', logKey, reply);
                callback(err, reply, vid);
            });
        }
        else {
            logger.info('%s Finished SetRequest. Result: %s', logKey, "Set Failed- No Existing Obj");
            callback(null, "Set Failed- No Existing Obj", 0);
        }
    });
};

var RemoveRequest = function (logKey, company, tenant, sessionId, reason, callback) {
    logger.info('%s ************************* Start RemoveRequest *************************', logKey);

    var key = util.format('Request:%s:%s:%s', company, tenant, sessionId);
    var tenantInt = parseInt(tenant);
    var companyInt = parseInt(company);
    var eventTime = new Date().toISOString();

    redisHandler.GetObj(logKey, key, function (err, obj) {
        if (err) {
            callback(err, "false");
        }else if(obj == null){
            callback("Error", "No Request found");
        }
        else {
            var requestObj = JSON.parse(obj);
            var tag = ["company_" + requestObj.Company, "tenant_" + requestObj.Tenant, "serverType_" + requestObj.ServerType, "requestType_" + requestObj.RequestType, "objtype_Request", "sessionid_" + requestObj.SessionId, "reqserverid_" + requestObj.RequestServerId, "priority_" + requestObj.Priority, "servingalgo_" + requestObj.ServingAlgo, "handlingalgo_" + requestObj.HandlingAlgo, "selectionalgo_" + requestObj.SelectionAlgo];
            var tempAttributeList = [];
            for (var i in requestObj.AttributeInfo) {
                var atts = requestObj.AttributeInfo[i].AttributeCode;
                for (var j in atts) {
                    tempAttributeList.push(atts[j]);
                }
            }
            var sortedAttributes = sortArray.sortData(tempAttributeList);
            for (var k in sortedAttributes) {
                tag.push("attribute_" + sortedAttributes[k]);
            }

            if (requestObj.ReqHandlingAlgo === "QUEUE") {
                if(reason == "NONE") {
                    var pubQueueId = requestObj.QueueId.replace(/:/g, "-");
                   // var pubMessage = util.format("EVENT:%s:%s:%s:%s:%s:%s:%s:%s:YYYY", tenant, company, "ARDS", "QUEUE", "ANSWERED", pubQueueId, "", requestObj.SessionId);
                    dashboardEventHandler.PublishEvent(logKey, tenantInt, companyInt, requestObj.BusinessUnit, "ARDS", "QUEUE", "ANSWERED", pubQueueId, "", requestObj.SessionId, eventTime);
                }
                //redisHandler.Publish(logKey, "events", pubMessage, function(){});
                reqQueueHandler.RemoveRequestFromQueue(logKey, company, tenant, requestObj.BusinessUnit, requestObj.QueueId, requestObj.SessionId, requestObj.RequestType, reason, function (err, result) {
                    if (err) {
                        logger.error(err);
                        callback(err, "false");
                    }else {
                        redisHandler.RemoveObj_V_T(logKey, key, tag, function (err, result) {
                            if (err) {
                                callback(err, "false");
                            }
                            else {
                                //var pubMessage = util.format("EVENT:%s:%s:%s:%s:%s:%s:%s:%s:YYYY", tenant, company, "ARDS", "REQUEST", "REMOVED", reason, "", sessionId);
                                dashboardEventHandler.PublishEvent(logKey, tenantInt, companyInt, requestObj.BusinessUnit, "ARDS", "REQUEST", "REMOVED", reason, "", sessionId, eventTime);

                                var reqStateKey = util.format('RequestState:%s:%s:%s', company, tenant, sessionId);
                                redisHandler.RemoveObj(logKey, reqStateKey, function () {
                                });
                                callback(null, result);
                            }
                        });
                    }
                    if(requestObj.QPositionEnable) {
                        reqQueueHandler.SendQueuePositionInfo(logKey, requestObj.QPositionUrl, requestObj.QueueId, requestObj.CallbackOption, function () {
                        });
                    }
                });
            }else {
                redisHandler.RemoveObj_V_T(logKey, key, tag, function (err, result) {
                    if (err) {
                        callback(err, "false");
                    }
                    else {
                        //var pubMessage = util.format("EVENT:%s:%s:%s:%s:%s:%s:%s:%s:YYYY", tenant, company, "ARDS", "REQUEST", "REMOVED", reason, "", sessionId);
                        dashboardEventHandler.PublishEvent(logKey, tenantInt, companyInt, requestObj.BusinessUnit, "ARDS", "REQUEST", "REMOVED", reason, "", sessionId, eventTime);
                        var reqStateKey = util.format('RequestState:%s:%s:%s', company, tenant, sessionId);
                        redisHandler.RemoveObj(logKey, reqStateKey, function () {
                        });
                        callback(null, result);
                    }
                });
            }
        }
    });
};

var RejectRequest = function (logKey, company, tenant, sessionId, reason, callback) {
    logger.info('%s ************************* Start RejectRequest *************************', logKey);
    logger.info("reject method hit :: SessionID: " + sessionId + " :: Reason: " + reason);

    var tenantInt = parseInt(tenant);
    var companyInt = parseInt(company);
    var key = util.format('Request:%s:%s:%s', company, tenant, sessionId);
    redisHandler.GetObj(logKey, key, function (err, obj) {
        if (err) {
            callback(err, "false");
        }
        else {
            var requestObj = JSON.parse(obj);
            /*var stags = ["company_" + company, "tenant_" + tenant, "handlingType_" + requestObj.RequestType, "handlingrequest_" + sessionId, "objtype_CSlotInfo"];

            redisHandler.SearchObj_T(logKey, stags, function (err, result) {
                if (err) {
                    console.log(err);
                }
                else {
                    if (result.length == 1) {
                        var csObj = result[0];
                        resourceHandler.UpdateSlotStateAvailable(logKey, company, tenant, csObj.HandlingType, csObj.ResourceId, csObj.SlotId, reason, "Reject", "Available", function (err, reply) {
                            if (err) {
                                console.log(err);
                            }
                        });
                    }
                }
            });*/
            if (reason == "NoSession" || reason == "ClientRejected") {
                var pubQueueId = requestObj.QueueId.replace(/:/g, "-");
                //var pubMessage = util.format("EVENT:%s:%s:%s:%s:%s:%s:%s:%s:YYYY", tenant, company, "ARDS", "QUEUE", "DROPPED", pubQueueId, "", requestObj.SessionId);
                var eventTime = new Date().toISOString();
                dashboardEventHandler.PublishEvent(logKey, tenantInt, companyInt, requestObj.BusinessUnit, "ARDS", "QUEUE", "DROPPED", pubQueueId, "", requestObj.SessionId, eventTime);
                RemoveRequest(logKey, company, tenant, sessionId, reason, function (err, result) {
                    if(reason == "NoSession"){
                        resourceHandler.UpdateSlotStateBySessionId(logKey, requestObj.Company, requestObj.Tenant, requestObj.RequestType, "", requestObj.SessionId, "Available", "NoSession", "", "inbound");
                    }
                    callback(err, result);
                });
            } else {
                SetRequestState(logKey, requestObj.Company, requestObj.Tenant, requestObj.SessionId, "QUEUED", function (err, result) {
                    if (err) {
                        logger.error(err);
                        callback(err, "false");
                    }
                    else {
                        reqQueueHandler.ReAddRequestToQueue(logKey, requestObj, function (err, result) {
                            if (err) {
                                logger.error(err);
                                callback(err, "false");
                            }
                            else if (result == "OK") {
                                logger.info("Request Readded to Queue Success");
                                callback(err, "true");
                            }
                            else {
                                logger.info("Request Readded to Queue Failed");
                                callback(err, "false");
                            }
                        });
                    }
                });
            }
        }
    });
};

var GetRequest = function (logKey, company, tenant, sessionId, callback) {
    logger.info('%s ************************* Start GetRequest *************************', logKey);

    var key = util.format('Request:%s:%s:%s', company, tenant, sessionId);
    redisHandler.GetObj_V(logKey, key, function (err, obj, vid) {
        logger.info('%s Finished GetRequest. Result: %s', logKey, obj);
        callback(err, obj, vid);
    });
};

var SearchRequestByTags = function (logKey, tags, callback) {
    logger.info('%s ************************* Start SearchRequestByTags *************************', logKey);

    if (Array.isArray(tags)) {
        tags.push("objtype_Request");
        redisHandler.SearchObj_V_T(logKey, tags, function (err, result) {
            logger.info('%s Finished SearchRequestByTags. Result: %s', logKey, result);
            callback(err, result);
        });
    }
    else {
        var e = new Error();
        e.message = "tags must be a string array";
        logger.info('%s Finished SearchRequestByTags. Result: %s', logKey, "tags must be a string array");
        callback(e, null);
    }
};

var AddProcessingRequest = function (logKey, requestObj, callback) {
    logger.info('%s ************************* Start AddProcessingRequest *************************', logKey);

    var key = util.format('ProcessingRequest:%d:%d:%s', requestObj.Company, requestObj.Tenant, requestObj.SessionId);
    var tag = ["company_" + requestObj.Company, "tenant_" + requestObj.Tenant, "serverType_" + requestObj.ServerType, "requestType_" + requestObj.RequestType, "objtype_ProcessingRequest", "sessionid_" + requestObj.SessionId, "reqserverid_" + requestObj.RequestServerId, "priority_" + requestObj.Priority, "servingalgo_" + requestObj.ServingAlgo, "handlingalgo" + requestObj.HandlingAlgo, "selectionalgo" + requestObj.SelectionAlgo];
    for (var i in requestObj.AttributeInfo) {
        tag.push("attribute_" + requestObj.AttributeInfo[i].AttributeCode);
    }
    var jsonObj = JSON.stringify(requestObj);

    redisHandler.AddObj_T(logKey, key, jsonObj, tag, function (err, reply) {
        logger.info('%s Finished AddProcessingRequest. Result: %s', logKey, reply);
        callback(err, reply);
    });
};

var GetProcessingRequest = function (logKey, company, tenant, sessionId, callback) {
    logger.info('%s ************************* Start GetProcessingRequest *************************', logKey);

    var key = util.format('ProcessingRequest:%s:%s:%s', company, tenant, sessionId);
    redisHandler.GetObj(logKey, key, function (err, obj) {
        logger.info('%s Finished GetProcessingRequest. Result: %s', logKey, obj);
        callback(err, obj);
    });
};

var RemoveProcessingRequest = function (logKey, company, tenant, sessionId, callback) {
    logger.info('%s ************************* Start RemoveProcessingRequest *************************', logKey);

    var key = util.format('ProcessingRequest:%s:%s:%s', company, tenant, sessionId);
    redisHandler.GetObj(logKey, key, function (err, obj) {
        if (err) {
            callback(err, "false");
        }
        else {
            var requestObj = JSON.parse(obj);
            var tag = ["company_" + requestObj.Company, "tenant_" + requestObj.Tenant, "serverType_" + requestObj.ServerType, "requestType_" + requestObj.RequestType, "objtype_Request", "sessionid_" + requestObj.SessionId, "reqserverid_" + requestObj.RequestServerId, "priority_" + requestObj.Priority, "servingalgo_" + requestObj.ServingAlgo, "handlingalgo" + requestObj.HandlingAlgo, "selectionalgo" + requestObj.SelectionAlgo];
            for (var i in requestObj.AttributeInfo) {
                tag.push("attribute_" + requestObj.AttributeInfo[i].AttributeCode);
            }

            redisHandler.RemoveObj_T(logKey, key, tag, function (err, result) {
                if (err) {
                    callback(err, "false");
                }
                else {
                    callback(null, result);
                }
            });
        }
    });
};

var SearchProcessingRequestByTags = function (logKey, tags, callback) {
    logger.info('%s ************************* Start SearchProcessingRequestByTags *************************', logKey);

    if (Array.isArray(tags)) {
        tags.push("objtype_ProcessingRequest");
        redisHandler.SearchObj_T(logKey, tags, function (err, result) {
            logger.info('%s Finished SearchProcessingRequestByTags. Result: %s', logKey, result);
            callback(err, result);
        });
    }
    else {
        var e = new Error();
        e.message = "tags must be a string array";
        logger.info('%s Finished SearchProcessingRequestByTags. Result: %s', logKey, "tags must be a string array");
        callback(e, null);
    }
};

var SetRequestState = function (logKey, company, tenant, sessionId, state, callback) {
    logger.info('%s ************************* Start SetRequestState *************************', logKey);

    var key = util.format('RequestState:%d:%d:%s', company, tenant, sessionId);
    redisHandler.SetObj(logKey, key, state, function (err, result) {
        if (err) {
            logger.error(err);
        }
        logger.info('%s Finished SetRequestState. Result: %s', logKey, result);
        callback(err, result);
    });
};

var GetRequestState = function (logKey, company, tenant, sessionId, callback) {
    logger.info('%s ************************* Start GetRequestState *************************', logKey);

    var key = util.format('RequestState:%d:%d:%s', company, tenant, sessionId);
    redisHandler.GetObj(logKey, key, function (err, result) {
        if (err) {
            logger.error(err);
        }
        logger.info('%s Finished GetRequestState. Result: %s', logKey, result);
        callback(err, result);
    });
};

module.exports.AddRequest = AddRequest;
module.exports.SetRequest = SetRequest;
module.exports.RemoveRequest = RemoveRequest;
module.exports.RejectRequest = RejectRequest;
module.exports.GetRequest = GetRequest;
module.exports.SearchRequestByTags = SearchRequestByTags;
module.exports.AddProcessingRequest = AddProcessingRequest;
module.exports.SetRequestState = SetRequestState;
module.exports.GetProcessingRequest = GetProcessingRequest;
module.exports.RemoveProcessingRequest = RemoveProcessingRequest;
module.exports.SearchProcessingRequestByTags = SearchProcessingRequestByTags;
module.exports.GetRequestState = GetRequestState;
