var redisHandler = require('./RedisHandler.js');
var util = require('util');
var logger = require("dvp-common/LogHandler/CommonLogHandler.js").logger;
var resourceService = require('./services/resourceService');
var scheduleWorkerHandler = require('./ScheduleWorkerHandler');
var moment = require('moment');

var SetResourceState = function (logKey, company, tenant, resourceId, resourceName, state, reason, callback) {
    logger.info('%s ************************* Start SetResourceState *************************', logKey);

    var StateKey = util.format('ResourceState:%d:%d:%s', company, tenant, resourceId);
    var internalAccessToken = util.format('%d:%d', tenant, company);

    validateState(logKey, tenant, company, resourceId, reason, function (isRequestValid, message, businessUnit) {

        if(isRequestValid){
            processState(logKey, StateKey, internalAccessToken, businessUnit, resourceId, resourceName, state, reason, function (err, resultObj) {
                if (err !== null) {
                    logger.error(err);
                    callback(err, undefined);
                }
                else {

                    var strObj = JSON.stringify(resultObj);
                    redisHandler.SetObj(logKey, StateKey, strObj, function (err, result) {
                        if (err !== null) {
                            logger.error(err);
                            callback(err, undefined);
                        }
                        else {
                            resourceService.AddResourceStatusChangeInfo(internalAccessToken, businessUnit, resourceId, "ResourceStatus", state, reason, {
                                SessionId: "",
                                Direction: ""
                            }, function (err, result, obj) {
                                if (err) {
                                    logger.error("AddResourceStatusChangeInfo Failed.", err);
                                } else {
                                    logger.info("AddResourceStatusChangeInfo Success.", obj);
                                }
                            });
                            if (reason && reason.toLowerCase() !== "endbreak" && reason.toLowerCase().indexOf('break') > -1) {
                                scheduleWorkerHandler.startBreak(company, tenant, resourceName, resourceId, reason, logKey);
                            }

                            if (reason && reason.toLowerCase() !== "break" && reason.toLowerCase().indexOf('endbreak') > -1) {
                                scheduleWorkerHandler.endBreak(company, tenant, resourceName, logKey);
                            }
                        }
                        callback(undefined, result);
                    });
                }
            });
        }else{
            callback(message, message);
        }

    });

};

var validateState = function (logKey, tenant, company, resourceId, reason, callback) {

    try {

        var reasonValue = reason.toLowerCase();
        if (reasonValue === "register" || reasonValue === "offline" || reasonValue === "unregister") {

            callback(true, null, 'default');

        } else {

            var resourceKey = util.format('Resource:%d:%d:%s', company, tenant, resourceId);
            redisHandler.GetObj(logKey, resourceKey, function (err, result) {
                if (err) {

                    callback(false, "Error occurred in processing state change request", 'default');

                } else {

                    if (result) {

                        var resourceObj = JSON.parse(result);
                        if (resourceObj) {

                            if (resourceObj.LoginTasks && (resourceObj.LoginTasks.length === 0 || resourceObj.LoginTasks.indexOf("CALL") === -1)) {

                                callback(true, "No login task found, proceed to state change", resourceObj.BusinessUnit);

                            } else {

                                if (resourceObj.ConcurrencyInfo && resourceObj.ConcurrencyInfo.length > 0) {

                                    var regexPattern = /CSlotInfo:([0-9]*:)*CALL:[0-9]*/g;
                                    var callSlots = resourceObj.ConcurrencyInfo.filter(function (cInfo) {
                                        return regexPattern.test(cInfo);
                                    });

                                    if (callSlots && callSlots.length > 0) {

                                        redisHandler.MGetObj(logKey, callSlots, function (err, results) {
                                            if (err) {

                                                callback(false, "Error occurred in processing state change request", resourceObj.BusinessUnit);

                                            } else {

                                                if (results) {

                                                    var slotAvailability = true;

                                                    for (var i = 0; i < results.length; i++) {

                                                        var slotObj = JSON.parse(results[i]);
                                                        slotAvailability = slotAvailability && (slotObj.State.toLowerCase() !== "connected" && !slotObj.FreezeAfterWorkTime);

                                                        if(slotObj.State.toLowerCase() === "reserved"){
                                                            var reservedTimeDiff = moment().diff(moment(slotObj.StateChangeTime), 'seconds');
                                                            slotAvailability = slotAvailability && (reservedTimeDiff > slotObj.MaxReservedTime);
                                                        }

                                                        if (!slotAvailability)
                                                            break;

                                                    }

                                                    if(slotAvailability){

                                                        callback(true, "All slots are free to accept the request", resourceObj.BusinessUnit);

                                                    }else{

                                                        callback(false, "Can't accept the request while on call", resourceObj.BusinessUnit);

                                                    }

                                                } else {

                                                    callback(false, "Error occurred in processing state change request", resourceObj.BusinessUnit);

                                                }

                                            }
                                        });

                                    } else {

                                        callback(true, "No call slot found, proceed to state change", resourceObj.BusinessUnit);

                                    }

                                } else {

                                    callback(true, "No concurrency info  found, proceed to state change", resourceObj.BusinessUnit);

                                }

                            }

                        } else {

                            callback(false, "Error occurred in processing state change request", 'default');

                        }

                    } else {

                        callback(false, "Error occurred in processing state change request", 'default');

                    }

                }
            });

        }

    } catch (ex) {
        callback(false, "Error occurred in processing state change request", 'default');
    }

};

var processState = function (logKey, stateKey, internalAccessToken, businessUnit, resourceId, resourceName, state, reason, callback) {

    var date = new Date();
    var statusObj = {ResourceName: resourceName, State: state, Reason: reason, StateChangeTime: date.toISOString()};
    redisHandler.GetObj(logKey, stateKey, function (err, statusStrObj) {
        if (err) {
            statusObj.Mode = 'Offline';
            return callback(null, statusObj);
        } else {
            if (statusStrObj) {
                var statusObjR = JSON.parse(statusStrObj);
                statusObj.Mode = statusObjR.Mode;

                if (statusObjR && statusObjR.State === "NotAvailable" && statusObjR.Reason.toLowerCase().indexOf('break') > -1) {
                    var duration = moment(statusObj.StateChangeTime).diff(moment(statusObjR.StateChangeTime), 'seconds');
                    resourceService.AddResourceStatusDurationInfo(internalAccessToken, businessUnit, resourceId, "ResourceStatus", statusObjR.State, statusObjR.Reason, '', '', duration, function () {
                        if (err) {
                            logger.error("AddResourceStatusDurationInfo Failed.", err);
                        } else {
                            logger.info("AddResourceStatusDurationInfo Success.");
                        }
                    });
                }

                if (state === "NotAvailable" && reason === "UnRegister") {
                    statusObj.Mode = "Offline";

                    if (statusObjR) {
                        resourceService.AddResourceStatusChangeInfo(internalAccessToken, businessUnit, resourceId, "ResourceStatus", statusObjR.State, "end" + statusObjR.Mode, {
                            SessionId: "",
                            Direction: ""
                        }, function (err, result, obj) {

                        });

                        if (statusObjR.State === "NotAvailable" && statusObjR.Reason.toLowerCase().indexOf('break') > -1) {
                            resourceService.AddResourceStatusChangeInfo(internalAccessToken, businessUnit, resourceId, "ResourceStatus", "Available", "endBreak", {
                                SessionId: "",
                                Direction: ""
                            }, function (err, result, obj) {

                            });
                            var duration1 = moment(statusObj.StateChangeTime).diff(moment(statusObjR.StateChangeTime), 'seconds');
                            resourceService.AddResourceStatusDurationInfo(internalAccessToken, businessUnit, resourceId, "ResourceStatus", statusObjR.State, statusObjR.Reason, '', '', duration1, function () {
                                if (err) {
                                    logger.error("AddResourceStatusDurationInfo Failed.", err);
                                } else {
                                    logger.info("AddResourceStatusDurationInfo Success.");
                                }
                            });

                            return callback(null, statusObj);
                        } else {
                            return callback(null, statusObj);
                        }
                    }else {
                        return callback(null, statusObj);
                    }
                } else if (reason === "Outbound" || reason === "Inbound" || reason === "Offline") {
                    statusObj.Mode = reason;

                    resourceService.AddResourceStatusChangeInfo(internalAccessToken, businessUnit, resourceId, "ResourceStatus", statusObjR.State, "end"+statusObjR.Mode, {
                        SessionId: "",
                        Direction: ""
                    }, function (err, result, obj) {
                        return callback(null, statusObj);
                    });


                } else {
                    return callback(null, statusObj);
                }
            } else {
                statusObj.Mode = 'Offline';
                return callback(null, statusObj);
            }
        }

    });

};

module.exports.SetResourceState = SetResourceState;