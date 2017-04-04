var util = require('util');
var redisHandler = require('./RedisHandler.js');
var requestHandler = require('./RequestHandler.js');
var infoLogger = require('./InformationLogger.js');
var rabbitMqHandler = require('./RabbitMQHandler.js');
var restClientHandler = require('./RestClient.js');
var config = require('config');

var AddRequestToQueue = function (logKey, request, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start AddRequestToQueue *************************', logKey);

    redisHandler.AddItemToListR(logKey, request.QueueId, request.SessionId, function (err, result) {
        if (err) {
            console.log(err);
            callback(err, "Failed", null);
        }
        else {
            if (parseInt(result) > 0) {
                var queuePosition = result;
                requestHandler.SetRequestState(logKey, request.Company, request.Tenant, request.SessionId, "QUEUED", function (err, result) {
                    console.log("set Request State QUEUED");
                });


                var hashKey = util.format('ProcessingHash:%d:%d', request.Company, request.Tenant);
                redisHandler.CheckHashFieldExists(logKey, hashKey, request.QueueId, function (err, hresult, result) {
                    if (err) {
                        console.log(err);
                        callback(err, "Failed", null);
                    }else {
                        if(result == "1"){
                            console.log("Hash Exists");
                            callback(err, "OK", queuePosition + 1);
                        }else {
                            SetNextProcessingItem(logKey, request.QueueId, hashKey, "CreateHash", function(result){
                                if((!hresult || hresult === "0") && config.Host.UseMsgQueue){
                                    rabbitMqHandler.Publish(logKey, "ARDS.Workers.Queue", hashKey);
                                }
                            });
                            console.log("Add item to Hash Success");
                            callback(err, "OK", queuePosition);
                            //redisHandler.AddItemToHash(logKey, hashKey, request.QueueId, request.SessionId, function (err, result) {
                            //    if (err) {
                            //        console.log(err);
                            //        callback(err, "Failed");
                            //    }
                            //    else {
                            //        /*requestHandler.SetRequestState(logKey, request.Company, request.Tenant, request.SessionId, "QUEUED", function (err, result) {
                            //         console.log("set Request State QUEUED");
                            //         if (hresult == "0") {
                            //         //rabbitMqHandler.Publish(logKey, "ARDS.Workers.Queue", hashKey);
                            //         }
                            //         });
                            //         var pubQueueId = request.QueueId.replace(/:/g, "-");
                            //         var pubMessage = util.format("EVENT:%d:%d:%s:%s:%s:%s:%s:%s:YYYY", request.Tenant, request.Company, "ARDS", "QUEUE", "ADDED", pubQueueId, "", request.SessionId);
                            //         redisHandler.Publish(logKey, "events", pubMessage, function () {
                            //         });*/
                            //
                            //
                            //        redisHandler.RemoveItemFromList(logKey, request.QueueId, request.SessionId, function (err, result) {
                            //            if (err) {
                            //                console.log(err);
                            //            } else {
                            //                console.log("Remove queue item seccess");
                            //            }
                            //        });
                            //
                            //
                            //
                            //    }
                            //
                            //});
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
                            SetNextProcessingItem(logKey, request.QueueId, hashKey, "CreateHash", function(result){
                                if((!hresult || hresult === "0") && config.Host.UseMsgQueue){
                                    rabbitMqHandler.Publish(logKey, "ARDS.Workers.Queue", hashKey);
                                }
                            });
                            console.log("Add item to Hash Success");
                            callback(err, "OK");
                            //redisHandler.AddItemToHash(logKey, hashKey, request.QueueId, request.SessionId, function (err, result) {
                            //    if (err) {
                            //        console.log(err);
                            //        callback(err, "Failed");
                            //    }
                            //    else {
                            //        /*requestHandler.SetRequestState(logKey, request.Company, request.Tenant, request.SessionId, "QUEUED", function (err, result) {
                            //         if (hresult == "0") {
                            //         //rabbitMqHandler.Publish(logKey, "ARDS.Workers.Queue", hashKey);
                            //         }
                            //         });*/
                            //
                            //
                            //
                            //        redisHandler.RemoveItemFromList(logKey, newQueueId, request.SessionId, function (err, result) {
                            //            if (err) {
                            //                console.log(err);
                            //            } else {
                            //                console.log("Remove queue item success");
                            //            }
                            //        });
                            //
                            //
                            //
                            //        console.log("Add item to Hash Success");
                            //        callback(err, "OK");
                            //    }
                            //});
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
                        SetNextProcessingItem(logKey,queueId,hashKey, sessionId, function(result){});
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
                                SetNextProcessingItem(logKey,queueId,hashKey, sessionId, function(result){});
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

var SetNextProcessingItem = function (logKey, queueId, processingHashId, currentSession, callback) {
    infoLogger.DetailLogger.log('info', '%s ************************* Start SetNextProcessingItem *************************', logKey);
    //var setNextLock = util.format("setNextLock.%s", queueId);
    //redisHandler.RLock(setNextLock, 1000, function (done) {
    redisHandler.GetHashValue(logKey, processingHashId, queueId, function (err, eSession) {
        if(err){
            console.log(err);
            callback("done");
        }else {
            if ((eSession && eSession == currentSession) || currentSession === "CreateHash") {

                var rejectedQueueId = GetRejectedQueueId(queueId);

                redisHandler.GetItemFromList(logKey, rejectedQueueId, function (err, nextRejectQueueItem) {
                    if (err) {
                        console.log(err);
                        callback("done");
                    }
                    else {
                        if (nextRejectQueueItem == "" || nextRejectQueueItem == null) {
                            redisHandler.GetItemFromList(logKey, queueId, function (err, nextQueueItem) {
                                if (err) {
                                    console.log(err);
                                    callback("done");
                                }
                                else {
                                    if (nextQueueItem == "" || nextQueueItem == null) {
                                        redisHandler.RemoveItemFromHash(logKey, processingHashId, queueId, function (err, result) {
                                            if (err) {
                                                console.log(err);
                                            }
                                            else {
                                                if (result === 1) {
                                                    console.log("Remove HashField Success.." + processingHashId + "::" + queueId);
                                                }
                                                else {
                                                    console.log("Remove HashField Failed.." + processingHashId + "::" + queueId);
                                                }
                                            }
                                            callback("done");
                                        });
                                    }
                                    else {
                                        redisHandler.AddItemToHash(logKey, processingHashId, queueId, nextQueueItem, function (err, result) {
                                            if (err) {
                                                console.log(err);
                                            }
                                            else {
                                                if (result === 1 || result === 0) {
                                                    console.log("Set HashField Success.." + processingHashId + "::" + queueId + "::" + nextQueueItem);
                                                    SendProcessingQueueInfo(logKey, queueId, nextQueueItem, function(){});
                                                }
                                                else {
                                                    console.log("Set HashField Failed.." + processingHashId + "::" + queueId + "::" + nextQueueItem);
                                                }
                                            }
                                            callback("done");
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
                                    if (result === 1 || result === 0) {
                                        console.log("Set HashField Success.." + processingHashId + "::" + queueId + "::" + nextRejectQueueItem);
                                    }
                                    else {
                                        console.log("Set HashField Failed.." + processingHashId + "::" + queueId + "::" + nextRejectQueueItem);
                                    }
                                }

                                callback("done");
                            });
                        }
                    }
                });

            } else {
                console.log("Session Mismatched ,ignore SetNextItem");
                callback("done");
            }
        }
    });
    //});
};

var GetRejectedQueueId = function(queueId){
    var splitQueueId = queueId.split(":");
    splitQueueId.splice(-1, 1);
    return util.format("%s:%s", splitQueueId.join(":"), "REJECTED");
};

var SendQueuePositionInfo = function(logKey, url, queueId, callbackOption, callback){
    infoLogger.DetailLogger.log('info', '%s:Queue: %s ************************* Start SendQueuePositionInfo *************************', logKey, queueId);
    redisHandler.GetRangeFromList(logKey, queueId, function(err, result){
        if(err){
            console.log(err);
        }else{
            if(result) {
                for (var i = 0; i < result.length; i++) {
                    var item = result[i];
                    if (item) {
                        var queuePosition = i + 2;
                        var body = {SessionId: item, QueueId: queueId, QueuePosition: queuePosition.toString()};
                        if (callbackOption == "GET") {
                            restClientHandler.DoGetDirect(url, body, function (err, res, result) {
                                if (err) {
                                    console.log(err);
                                } else {
                                    console.log("SendQueuePositionInfo: %s", result);
                                }
                            });
                        } else {
                            restClientHandler.DoPostDirect(url, body, function (err, res, result) {
                                if (err) {
                                    console.log(err);
                                } else {
                                    console.log("SendQueuePositionInfo: %s", result);
                                }
                            });
                        }
                    }
                }
            }
        }
    });
    callback("done");
};

var SendProcessingQueueInfo = function(logKey, queueId, sessionId, callback){
    infoLogger.DetailLogger.log('info', '%s:Queue: %s ************************* Start SendProcessingQueueInfo *************************', logKey, queueId);

    var splitQueueId = queueId.split(":");
    if(splitQueueId && splitQueueId.length > 3) {
        var reqKey = util.format('Request:%s:%s:%s', splitQueueId[1], splitQueueId[2], sessionId);
        redisHandler.GetObj(logKey, reqKey, function(err, strObj){
            if(!err && strObj) {
                var obj = JSON.parse(strObj);
                if(obj) {
                    var body = {SessionId: sessionId, QueueId: queueId, QueuePosition: "1"};
                    if (obj.CallbackOption == "GET") {
                        restClientHandler.DoGetDirect(obj.QPositionUrl, body, function (err, res, result) {
                            if (err) {
                                console.log(err);
                            } else {
                                console.log("SendQueuePositionInfo: %s", result);
                            }
                        });
                    } else {
                        restClientHandler.DoPostDirect(url, body, function (err, res, result) {
                            if (err) {
                                console.log(err);
                            } else {
                                console.log("SendQueuePositionInfo: %s", result);
                            }
                        });
                    }
                }
            }
        });

    }

    callback("done");
};

module.exports.AddRequestToQueue = AddRequestToQueue;
module.exports.ReAddRequestToQueue = ReAddRequestToQueue;
module.exports.RemoveRequestFromQueue = RemoveRequestFromQueue;
module.exports.GetNextRequestToProcess = GetNextRequestToProcess;
module.exports.SetNextProcessingItem = SetNextProcessingItem;
module.exports.SendQueuePositionInfo = SendQueuePositionInfo;