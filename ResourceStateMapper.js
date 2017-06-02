var redisHandler = require('./RedisHandler.js');
var util = require('util');
var infoLogger = require('./InformationLogger.js');
var resourceService = require('./services/resourceService');
var scheduleWorkerHandler = require('./ScheduleWorkerHandler');

var SetResourceState = function (logKey, company, tenant, resourceId, resourceName, state, reason, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start SetResourceState *************************', logKey);

    var StateKey = util.format('ResourceState:%d:%d:%s', company, tenant, resourceId);
    var internalAccessToken = util.format('%d:%d', tenant,company);
    processState(logKey, StateKey, internalAccessToken, resourceId, resourceName, state, reason, function (err, resultObj) {
        if (err != null) {
            console.log(err);
        }
        else {
            var date = new Date();
            resultObj.StateChangeTime = date.toISOString();
            var strObj = JSON.stringify(resultObj);
            redisHandler.SetObj(logKey, StateKey, strObj, function (err, result) {
                if (err != null) {
                    console.log(err);
                }
                else {
                    resourceService.AddResourceStatusChangeInfo(internalAccessToken, resourceId, "ResourceStatus", state, reason, {SessionId:"", Direction:""}, function(err, result, obj){
                        if(err){
                            console.log("AddResourceStatusChangeInfo Failed.", err);
                        }else{
                            console.log("AddResourceStatusChangeInfo Success.", obj);
                        }
                    });
                    if(reason && reason.toLowerCase() !== "endbreak" && reason.toLowerCase().indexOf('break') > -1){
                        scheduleWorkerHandler.StartBreak(company, tenant, resourceName,resourceId, logKey);
                    }

                    if(reason && reason.toLowerCase() !== "break" && reason.toLowerCase().indexOf('endbreak') > -1){
                        scheduleWorkerHandler.EndBreak(resourceId);
                    }
                }
                callback(err, result);
            });
        }
    });
};

var processState = function (logKey, stateKey, internalAccessToken, resourceId, resourceName, state, reason, callback) {
    var statusObj = {ResourceName: resourceName, State: state, Reason: reason};

    redisHandler.GetObj(logKey, stateKey, function (err, statusStrObj) {
        if(err){
            statusObj.Mode = 'Offline';
            callback(null, statusObj);
        }else{
            if(statusStrObj) {
                var statusObjR = JSON.parse(statusStrObj);
                statusObj.Mode = statusObjR.Mode;
                if(state === "NotAvailable" && reason === "UnRegister") {
                    statusObj.Mode = "Offline";
                    if (statusObjR && statusObjR.State === "NotAvailable") {
                        resourceService.AddResourceStatusChangeInfo(internalAccessToken, resourceId, "ResourceStatus", "Available", "EndBreak", {SessionId:"", Direction:""}, function (err, result, obj) {
                            callback(null, statusObj);
                        });
                    } else {
                        callback(null, statusObj);
                    }
                }else if(reason === "Outbound" || reason === "Inbound" || reason === "Offline"){
                    statusObj.Mode = reason;
                    callback(null, statusObj);
                }else{
                    callback(null, statusObj);
                }
            }else{
                statusObj.Mode = 'Offline';
                callback(null, statusObj);
            }
        }

    });

};

module.exports.SetResourceState = SetResourceState;