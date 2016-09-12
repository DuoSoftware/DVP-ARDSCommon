var util = require('util');
var redisHandler = require('./RedisHandler.js');
var requestHandler = require('./RequestHandler.js');
var infoLogger = require('./InformationLogger.js');
var rabbitMqHandler = require('./RabbitMQHandler.js');
var restClientHandler = require('./RestClient.js');

var AddRequestToQueue = function (logKey, request, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start AddRequestToQueue *************************', logKey);

    redisHandler.AddItemToListR(logKey, request.QueueId, request.SessionId, function (err, result) {
        if (err) {
            console.log(err);
            callback(err, "Failed");
        }
        else {
            if (parseInt(result) > 0) {
                requestHandler.SetRequestState(logKey, request.Company, request.Tenant, request.SessionId, "QUEUED", function (err, result) {
                    console.log("set Request State QUEUED");
                });


                var hashKey = util.format('ProcessingHash:%d:%d', request.Company, request.Tenant);
                redisHandler.CheckHashFieldExists(logKey, hashKey, request.QueueId, function (err, hresult, result) {
                    if (err) {
                        console.log(err);
                        callback(err, "Failed");
                    }else {
                        if(result == "1"){
                            console.log("Hash Exists");
                            callback(err, "OK");
                        }else {
                            redisHandler.AddItemToHash(logKey, hashKey, request.QueueId, request.SessionId, function (err, result) {
                                if (err) {
                                    console.log(err);
                                    callback(err, "Failed");
                                }
                                else {
                                    /*requestHandler.SetRequestState(logKey, request.Company, request.Tenant, request.SessionId, "QUEUED", function (err, result) {
                                     console.log("set Request State QUEUED");
                                     if (hresult == "0") {
                                     //rabbitMqHandler.Publish(logKey, "ARDS.Workers.Queue", hashKey);
                                     }
                                     });
                                     var pubQueueId = request.QueueId.replace(/:/g, "-");
                                     var pubMessage = util.format("EVENT:%d:%d:%s:%s:%s:%s:%s:%s:YYYY", request.Tenant, request.Company, "ARDS", "QUEUE", "ADDED", pubQueueId, "", request.SessionId);
                                     redisHandler.Publish(logKey, "events", pubMessage, function () {
                                     });*/


                                    redisHandler.RemoveItemFromList(logKey, request.QueueId, request.SessionId, function (err, result) {
                                        if (err) {
                                            console.log(err);
                                        } else {
                                            console.log("Remove queue item seccess");
                                        }
                                    });


                                    console.log("Add item to Hash Success");
                                    callback(err, "OK");
                                }

                            });
                        }
                    }
                });



                var pubQueueId = request.QueueId.replace(/:/g, "-");
                var pubMessage = util.format("EVENT:%d:%d:%s:%s:%s:%s:%s:%s:YYYY", request.Tenant, request.Company, "ARDS", "QUEUE", "ADDED", pubQueueId, "", request.SessionId);
                redisHandler.Publish(logKey, "events", pubMessage, function(){});
            }
            else {
                callback(err, "Failed");
            }
        }
    });


};

var ReAddRequestToQueue = function (logKey, request, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start ReAddRequestToQueue *************************', logKey);

    var newQueueId = GetRejectedQueueId(request.QueueId);
    redisHandler.AddItemToListR(logKey, newQueueId, request.SessionId, function (err, result) {
        if (err) {
            console.log(err);
            callback(err, "Failed");
        }
        else {
            if (parseInt(result) > 0) {
                requestHandler.SetRequestState(logKey, request.Company, request.Tenant, request.SessionId, "QUEUED", function (err, result) {
                });



                var hashKey = util.format('ProcessingHash:%d:%d', request.Company, request.Tenant);
                redisHandler.CheckHashFieldExists(logKey, hashKey, request.QueueId, function (err, hresult, result) {
                    if (err) {
                        console.log(err);
                        callback(err, "Failed");
                    }else {
                        if (result == "1") {
                            console.log("Hash Exsists");
                            callback(err, "OK");
                        }else{
                            redisHandler.AddItemToHash(logKey, hashKey, request.QueueId, request.SessionId, function (err, result) {
                                if (err) {
                                    console.log(err);
                                    callback(err, "Failed");
                                }
                                else {
                                    /*requestHandler.SetRequestState(logKey, request.Company, request.Tenant, request.SessionId, "QUEUED", function (err, result) {
                                     if (hresult == "0") {
                                     //rabbitMqHandler.Publish(logKey, "ARDS.Workers.Queue", hashKey);
                                     }
                                     });*/



                                    redisHandler.RemoveItemFromList(logKey, newQueueId, request.SessionId, function (err, result) {
                                        if (err) {
                                            console.log(err);
                                        } else {
                                            console.log("Remove queue item success");
                                        }
                                    });



                                    console.log("Add item to Hash Success");
                                    callback(err, "OK");
                                }
                            });
                        }
                    }
                });




            }
            else {
                callback(err, "Failed");
            }
        }
    });


};

var RemoveRequestFromQueue = function (logKey, company, tenant, queueId, sessionId, reason, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start RemoveRequestFromQueue *************************', logKey);

    redisHandler.RemoveItemFromList(logKey, queueId, sessionId, function (err, result) {
        if (err) {
            console.log(err);
        }else{
            if(result >0) {
                var pubQueueId = queueId.replace(/:/g, "-");
                var pubMessage = util.format("EVENT:%s:%s:%s:%s:%s:%s:%s:%s:YYYY", tenant, company, "ARDS", "QUEUE", "REMOVED", pubQueueId, "", sessionId);
                redisHandler.Publish(logKey, "events", pubMessage, function () {
                });
                var hashKey = util.format('ProcessingHash:%s:%s', company, tenant);
                redisHandler.GetHashValue(logKey,hashKey, queueId, function(err, eSession){
                    if(eSession && eSession == sessionId){
                        //redisHandler.RemoveItemFromHash(logKey,hashKey,queueId,function(){});
                        SetNextProcessingItem(logKey,queueId,hashKey, sessionId);
                    }
                });
                callback(err, result);
            }else{
                var rejectedQueueId = GetRejectedQueueId(queueId);
                redisHandler.RemoveItemFromList(logKey, rejectedQueueId, sessionId, function (err, result) {
                    if (err) {
                        console.log(err);
                    }else{
                        var pubQueueId = queueId.replace(/:/g, "-");
                        var pubMessage = util.format("EVENT:%s:%s:%s:%s:%s:%s:%s:%s:YYYY", tenant, company, "ARDS", "QUEUE", "REMOVED", pubQueueId, "", sessionId);
                        redisHandler.Publish(logKey, "events", pubMessage, function () {
                        });
                        var hashKey = util.format('ProcessingHash:%s:%s', company, tenant);
                        redisHandler.GetHashValue(logKey,hashKey, queueId, function(err, eSession){
                            if(eSession && eSession == sessionId){
                                //redisHandler.RemoveItemFromHash(logKey,hashKey,queueId,function(){});
                                SetNextProcessingItem(logKey,queueId,hashKey, sessionId);
                            }
                        });
                        callback(err, result);
                    }
                });
            }
        }
    });
};

var GetNextRequestToProcess = function (logKey, queueId, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start GetNextRequestToProcess *************************', logKey);
    var rejectedQueueId = GetRejectedQueueId(queueId);

    redisHandler.GetItemFromList(logKey, rejectedQueueId, function (err, rejectListResult) {
        if (err) {
            console.log(err);
        }else{
            if(rejectListResult == ""){
                redisHandler.GetItemFromList(logKey, queueId, function (err, result) {
                    if (err) {
                        console.log(err);
                    }
                    callback(err, result);
                });
            }else{
                callback(err, rejectListResult);
            }
        }
    });
};

var SetNextProcessingItem = function (logKey, queueId, processingHashId, currentSession) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start SetNextProcessingItem *************************', logKey);
    var setNextLock = util.format("setNextLock.%s", queueId);
    redisHandler.RLock(setNextLock, 1000, function (done) {
        redisHandler.GetHashValue(logKey, processingHashId, queueId, function (err, eSession) {
            if(err){
                console.log(err);
                done();
            }else {
                if (eSession && eSession == currentSession) {

                    var rejectedQueueId = GetRejectedQueueId(queueId);

                    redisHandler.GetItemFromList(logKey, rejectedQueueId, function (err, nextRejectQueueItem) {
                        if (err) {
                            console.log(err);
                            done();
                        }
                        else {
                            if (nextRejectQueueItem == "" || nextRejectQueueItem == null) {
                                redisHandler.GetItemFromList(logKey, queueId, function (err, nextQueueItem) {
                                    if (err) {
                                        console.log(err);
                                        done();
                                    }
                                    else {
                                        if (nextQueueItem == "" || nextQueueItem == null) {
                                            redisHandler.RemoveItemFromHash(logKey, processingHashId, queueId, function (err, result) {
                                                if (err) {
                                                    console.log(err);
                                                }
                                                else {
                                                    if (result == "1") {
                                                        console.log("Remove HashField Success.." + _processingHash + "::" + _queueId);
                                                    }
                                                    else {
                                                        console.log("Remove HashField Failed.." + _processingHash + "::" + _queueId);
                                                    }
                                                }
                                                done();
                                            });
                                        }
                                        else {
                                            redisHandler.AddItemToHash(logKey, processingHashId, queueId, nextQueueItem, function (err, result) {
                                                if (err) {
                                                    console.log(err);
                                                }
                                                else {
                                                    if (result == "1") {
                                                        console.log("Set HashField Success.." + _processingHash + "::" + _queueId + "::" + nextQueueItem);
                                                    }
                                                    else {
                                                        console.log("Set HashField Failed.." + _processingHash + "::" + _queueId + "::" + nextQueueItem);
                                                    }
                                                }
                                                done();
                                            });
                                        }
                                    }
                                });
                            } else {
                                redisHandler.AddItemToHash(logKey, processingHashId, queueId, nextRejectQueueItem, function (err, result) {
                                    if (err) {
                                        console.log(err);
                                    }
                                    else {
                                        if (result == "1") {
                                            console.log("Set HashField Success.." + _processingHash + "::" + _queueId + "::" + nextQueueItem);
                                        }
                                        else {
                                            console.log("Set HashField Failed.." + _processingHash + "::" + _queueId + "::" + nextQueueItem);
                                        }
                                    }

                                    done();
                                });
                            }
                        }
                    });

                } else {
                    console.log("Session Mismatched ,ignore SetNextItem");
                    done();
                }
            }
        });
    });
};

var GetRejectedQueueId = function(queueId){
    var splitQueueId = queueId.split(":");
    var tempSplitQueueId = splitQueueId.splice(-1, 1);
    return util.format("%s:%s", tempSplitQueueId.join(":"), "REJECTED");
};

var SendQueuePositionInfo = function(logKey, url, queueId, callback){
    infoLogger.DetailLogger.log('info', '%s:Queue: %s ************************* Start SendQueuePositionInfo *************************', logKey, queueId);
    redisHandler.GetRangeFromList(logKey, queueId, function(err, result){
        if(err){
            console.log(err);
        }else{
            for (var i=0; i< result.length; i++) {
                var item = result[i];
                if(item){
                    var body = {SessionId: item, QueueId: queueId, QueuePosition: i+1};
                    restClientHandler.DoPostDirect(url, body, function (err, res, result){
                        if(err){
                            console.log(err);
                        }else{
                            console.log("SendQueuePositionInfo: %s: %s", item, result);
                        }
                    });
                }
            }
        }
    });
    callback("done");
};

module.exports.AddRequestToQueue = AddRequestToQueue;
module.exports.ReAddRequestToQueue = ReAddRequestToQueue;
module.exports.RemoveRequestFromQueue = RemoveRequestFromQueue;
module.exports.GetNextRequestToProcess = GetNextRequestToProcess;
module.exports.SetNextProcessingItem = SetNextProcessingItem;
module.exports.SendQueuePositionInfo = SendQueuePositionInfo;