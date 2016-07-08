var redisHandler = require('./RedisHandler.js');
var util = require('util');
var infoLogger = require('./InformationLogger.js');
var resourceService = require('./services/resourceService');

var SetResourceState = function (logKey, company, tenant, resourceId, state, reason, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start SetResourceState *************************', logKey);

    var StateKey = util.format('ResourceState:%d:%d:%s', company, tenant, resourceId);
    processState(logKey, StateKey, state, reason, function (err, resultObj) {
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
                    var internalAccessToken = util.format('%d:%d', tenant,company);
                    resourceService.AddResourceStatusChangeInfo(internalAccessToken, resourceId, "ResourceStatus", state, reason, "", function(err, result, obj){
                        if(err){
                            console.log("AddResourceStatusChangeInfo Failed.", err);
                        }else{
                            console.log("AddResourceStatusChangeInfo Success.", obj);
                        }
                    });
                }
                callback(err, result);
            });
        }
    });
};

var processState = function (logKey, stateKey, state, reason, callback) {
    var statusObj = {State: state, Reason: reason};
    if(state === "NotAvailable" && reason === "UnRegister") {
        redisHandler.GetObj(logKey, stateKey, function (err, statusObjR) {
            if (statusObjR && statusObjR.State === "NotAvailable") {
                resourceService.AddResourceStatusChangeInfo(internalAccessToken, resourceId, "ResourceStatus", "Available", "EndBreak", "", function (err, result, obj) {
                    callback(null, statusObj);
                });
            }else{
                callback(null, statusObj);
            }
        });
    }else{
        callback(null, statusObj);
    }

};

module.exports.SetResourceState = SetResourceState;