var util = require('util');
var redisHandler = require('./RedisHandler.js');
var dashboardEventHandler = require('./DashboardEventHandler');
var requestHandler = require('./RequestHandler.js');
var rabbitMqHandler = require('./RabbitMQHandler.js');
var restClientHandler = require('./RestClient.js');
var config = require('config');
var logger = require('dvp-common/LogHandler/CommonLogHandler.js').logger;
var async = require('async');
var uuidv4 = require('uuid/v4');


var AddRequestToQueue = function (logKey, request, callback) {
    logger.info('%s ************************* Start AddRequestToQueue *************************', logKey);

    redisHandler.AddItemToListR(logKey, request.QueueId, request.SessionId, function (err, result) {
        if (err) {
            logger.error("AddItemToListR error:: "+err);
            callback(err, "Failed", null);
        }
        else {
            if (parseInt(result) > 0) {
                var queuePosition = result;
                requestHandler.SetRequestState(logKey, request.Company, request.Tenant, request.SessionId, "QUEUED", function (err, result) {
                    logger.info("set Request State QUEUED");
                });

                var pubQueueId = request.QueueId.replace(/:/g, "-");
                var eventTime = new Date().toISOString();
                dashboardEventHandler.PublishEvent(logKey, request.Tenant, request.Company, request.BusinessUnit, "ARDS", "QUEUE", "ADDED", pubQueueId, "", request.SessionId, eventTime);

                var hashKey = util.format('ProcessingHash:%d:%d:%s', request.Company, request.Tenant, request.RequestType);
                SetNextProcessingItem(logKey, request.QueueId, hashKey, "CreateHash", function (result) {
                    logger.info("Add item to Hash Success");
                    callback(err, "OK", queuePosition);
                });


                //var redLokKey = util.format('lock:%s:%s', hashKey, request.QueueId);

                // redisHandler.RLock.lock(redLokKey, 500).then(function(lock) {
                //     redisHandler.CheckHashFieldExists(logKey, hashKey, request.QueueId, function (err, hresult, result) {
                //         if (err) {
                //             console.log(err);
                //             lock.unlock()
                //                 .catch(function (err) {
                //                     console.error(err);
                //                 });
                //             callback(err, "Failed", null);
                //         } else {
                //             if (result == "1") {
                //                 console.log("Hash Exists");
                //                 lock.unlock()
                //                     .catch(function (err) {
                //                         console.error(err);
                //                     });
                //                 callback(err, "OK", queuePosition + 1);
                //             } else {
                //                 SetNextProcessingItem(logKey, request.QueueId, hashKey, "CreateHash", function (result) {
                //                     if ((!hresult || hresult === "0") && config.Host.UseMsgQueue === 'true') {
                //                         rabbitMqHandler.Publish(logKey, "ARDS.Workers.Queue", hashKey);
                //                     }
                //
                //                     console.log("Add item to Hash Success");
                //                     lock.unlock()
                //                         .catch(function (err) {
                //                             console.error(err);
                //                         });
                //                     callback(err, "OK", queuePosition);
                //                 });
                //
                //             }
                //         }
                //     });
                // });


            }
            else {
                callback(err, "Failed");
            }
        }
    });


};

var ReAddRequestToQueue = function (logKey, request, callback) {
    logger.info('%s ************************* Start ReAddRequestToQueue *************************', logKey);

    var newQueueId = GetRejectedQueueId(request.QueueId);
    redisHandler.AddItemToListR(logKey, newQueueId, request.SessionId, function (err, result) {
        if (err) {
            logger.error("AddItemToListR error:: "+err);
            callback(err, "Failed");
        }
        else {
            if (parseInt(result) > 0) {
                requestHandler.SetRequestState(logKey, request.Company, request.Tenant, request.SessionId, "QUEUED", function (err, result) {
                });


                var hashKey = util.format('ProcessingHash:%d:%d:%s', request.Company, request.Tenant, request.RequestType);
                SetNextProcessingItem(logKey, request.QueueId, hashKey, "CreateHash", function (result) {
                    logger.info("Add item to Hash Success");
                    callback(err, "OK");
                });

                // var redLokKey = util.format('lock:%s:%s', hashKey, request.QueueId);
                //
                // redisHandler.RLock.lock(redLokKey, 500).then(function(lock) {
                //     redisHandler.CheckHashFieldExists(logKey, hashKey, request.QueueId, function (err, hresult, result) {
                //         if (err) {
                //             console.log(err);
                //             lock.unlock()
                //                 .catch(function (err) {
                //                     console.error(err);
                //                 });
                //             callback(err, "Failed");
                //         } else {
                //             if (result == "1") {
                //                 console.log("Hash Exsists");
                //                 lock.unlock()
                //                     .catch(function (err) {
                //                         console.error(err);
                //                     });
                //                 callback(err, "OK");
                //             } else {
                //                 SetNextProcessingItem(logKey, request.QueueId, hashKey, "CreateHash", function (result) {
                //                     if ((!hresult || hresult === "0") && config.Host.UseMsgQueue === 'true') {
                //                         rabbitMqHandler.Publish(logKey, "ARDS.Workers.Queue", hashKey);
                //                     }
                //                     lock.unlock()
                //                         .catch(function (err) {
                //                             console.error(err);
                //                         });
                //                     console.log("Add item to Hash Success");
                //                     callback(err, "OK");
                //                 });
                //
                //             }
                //         }
                //     });
                // });


            }
            else {
                callback(err, "Failed");
            }
        }
    });


};

var RemoveRequestFromQueue = function (logKey, company, tenant, businessUnit, queueId, sessionId, requestType, reason, callback) {
    logger.info('%s ************************* Start RemoveRequestFromQueue *************************', logKey);

    var tenantInt = parseInt(tenant);
    var companyInt = parseInt(company);
    var eventTime = new Date().toISOString();

    redisHandler.RemoveItemFromList(logKey, queueId, sessionId, function (err, result) {
        if (err) {
            logger.error("RemoveItemFromList error:: "+err);
            callback(err, result);
        } else {
            if (result > 0) {
                var pubQueueId = queueId.replace(/:/g, "-");
                //var pubMessage = util.format("EVENT:%s:%s:%s:%s:%s:%s:%s:%s:YYYY", tenant, company, "ARDS", "QUEUE", "REMOVED", pubQueueId, "", sessionId);
                dashboardEventHandler.PublishEvent(logKey, tenantInt, companyInt, businessUnit, "ARDS", "QUEUE", "REMOVED", pubQueueId, "", sessionId, eventTime);

                var hashKey = util.format('ProcessingHash:%s:%s:%s', company, tenant, requestType);
                SetNextProcessingItem(logKey, queueId, hashKey, sessionId, function (result) {
                    logger.info("Add item to Hash Success");
                    callback(err, result);
                });
                // var redLokKey = util.format('lock:%s:%s', hashKey, queueId);
                //
                // redisHandler.RLock.lock(redLokKey, 500).then(function (lock) {
                //     redisHandler.GetHashValue(logKey, hashKey, queueId, function (err, eSession) {
                //         if (eSession && eSession === sessionId) {
                //             //redisHandler.RemoveItemFromHash(logKey,hashKey,queueId,function(){});
                //             SetNextProcessingItem(logKey, queueId, hashKey, sessionId, function (result) {
                //                 lock.unlock()
                //                     .catch(function (err) {
                //                         console.error(err);
                //                     });
                //                 callback(err, result);
                //             });
                //         } else {
                //             lock.unlock()
                //                 .catch(function (err) {
                //                     console.error(err);
                //                 });
                //             callback(err, result);
                //         }
                //     });
                // });
            } else {
                var rejectedQueueId = GetRejectedQueueId(queueId);
                redisHandler.RemoveItemFromList(logKey, rejectedQueueId, sessionId, function (err, result) {
                    if (err) {
                        logger.error("RemoveItemFromList error:: "+err);
                        callback(err, result);
                    } else {
                        var pubQueueId = queueId.replace(/:/g, "-");
                        //var pubMessage = util.format("EVENT:%s:%s:%s:%s:%s:%s:%s:%s:YYYY", tenant, company, "ARDS", "QUEUE", "REMOVED", pubQueueId, "", sessionId);
                        dashboardEventHandler.PublishEvent(logKey, tenantInt, companyInt, businessUnit, "ARDS", "QUEUE", "REMOVED", pubQueueId, "", sessionId, eventTime);

                        var hashKey = util.format('ProcessingHash:%s:%s:%s', company, tenant, requestType);
                        SetNextProcessingItem(logKey, queueId, hashKey, sessionId, function (result) {
                            logger.info("Add item to Hash Success");
                            callback(err, result);
                        });

                        // var redLokKey = util.format('lock:%s:%s', hashKey, queueId);
                        //
                        // redisHandler.RLock.lock(redLokKey, 500).then(function (lock) {
                        //     redisHandler.GetHashValue(logKey, hashKey, queueId, function (err, eSession) {
                        //         if (eSession && eSession === sessionId) {
                        //             //redisHandler.RemoveItemFromHash(logKey,hashKey,queueId,function(){});
                        //             SetNextProcessingItem(logKey, queueId, hashKey, sessionId, function (result) {
                        //                 lock.unlock()
                        //                     .catch(function (err) {
                        //                         console.error(err);
                        //                     });
                        //                 callback(err, result);
                        //             });
                        //         } else {
                        //             lock.unlock()
                        //                 .catch(function (err) {
                        //                     console.error(err);
                        //                 });
                        //             callback(err, result);
                        //         }
                        //     });
                        // });
                    }
                });
            }
        }
    });
};

var GetNextRequestToProcess = function (logKey, queueId, callback) {
    logger.info('%s ************************* Start GetNextRequestToProcess *************************', logKey);
    var rejectedQueueId = GetRejectedQueueId(queueId);

    redisHandler.GetItemFromList(logKey, rejectedQueueId, function (err, rejectListResult) {
        if (err) {
            logger.error("GetItemFromList error:: "+err);
        } else {
            if (rejectListResult == "") {
                redisHandler.GetItemFromList(logKey, queueId, function (err, result) {
                    if (err) {
                        logger.error("GetItemFromList error:: "+err);
                    }
                    callback(err, result);
                });
            } else {
                callback(err, rejectListResult);
            }
        }
    });
};

/*
var SetNextProcessingItem = function (logKey, queueId, processingHashId, currentSession, callback) {
    logger.info('%s ************************* Start SetNextProcessingItem *************************', logKey);
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
                                                    //SendProcessingQueueInfo(logKey, queueId, nextQueueItem, function(){});
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
*/

var SetNextProcessingItem = function (logKey, queueId, processingHashId, currentSession, callback) {

    var redLokKey = util.format('lock:%s:%s', processingHashId, queueId);

    redisHandler.RLock.lock(redLokKey, 500).then(function (lock) {

        var queueExecuteCount = util.format('ExecCount:%s', queueId);
        redisHandler.GetObj(logKey, queueExecuteCount, function (err, exeCountStr) {

            var exeCount = (err || !exeCountStr) ? 1 : parseInt(exeCountStr);

            var hscanPattern = util.format('%s:*', queueId);
            redisHandler.HScanHash(logKey, processingHashId, hscanPattern, function (processingHashDetail) {

                if (processingHashDetail) {

                    var RemoveItemFromProcessingHash = function (field, callback) {
                        redisHandler.RemoveItemFromHash(logKey, processingHashId, field, function (err, result) {
                            if (err) {
                                logger.error('RemoveItemFromHash failed:: ' + err);
                                callback("done");
                            }
                            else {
                                if (result === 1) {
                                    logger.info("Remove HashField Success.." + processingHashId + "::" + field);
                                }
                                else {
                                    logger.info("Remove HashField Failed.." + processingHashId + "::" + field);
                                }

                                callback("done");
                            }
                        });
                    };

                    var AddItemToProcessingHash = function (field, value, callback) {
                        redisHandler.AddItemToHash(logKey, processingHashId, field, value, function (err, result) {
                            if (err) {
                                logger.error('AddItemToHash failed:: ' + err);
                                callback("done");
                            }
                            else {
                                if (result === 1 || result === 0) {
                                    logger.info("Set HashField Success.." + processingHashId + "::" + field + "::" + value);
                                }
                                else {
                                    logger.info("Set HashField Failed.." + processingHashId + "::" + field + "::" + value);
                                }

                                callback("done");
                            }
                        });
                    };

                    var GetNextItemToProcess = function (callback) {

                        var rejectedQueueId = GetRejectedQueueId(queueId);
                        redisHandler.GetItemFromList(logKey, rejectedQueueId, function (err, nextRejectQueueItem) {
                            if (err) {
                                logger.error('GetNextItemToProcess failed:: ' + err);
                                callback(null);
                            }
                            else {
                                if (nextRejectQueueItem == "" || nextRejectQueueItem == null) {
                                    redisHandler.GetItemFromList(logKey, queueId, function (err, nextQueueItem) {
                                        if (err) {
                                            logger.error('GetNextItemToProcess failed:: ' + err);
                                            callback(null);
                                        }
                                        else {
                                            callback(nextQueueItem);
                                        }
                                    });
                                } else {
                                    callback(nextRejectQueueItem);
                                }
                            }
                        });
                    };

                    var SetNextItem = function (callback) {

                        if (processingHashDetail.MatchingValues && processingHashDetail.MatchingValues.indexOf(currentSession) > -1) {

                            var hashRecords = processingHashDetail.MatchingKeyValues.filter(function (item) {
                                return item.Value === currentSession;
                            });

                            if (hashRecords && hashRecords.length > 0) {

                                var fieldKey = hashRecords[0].Field;

                                GetNextItemToProcess(function (nextItem) {

                                    if (nextItem) {
                                        AddItemToProcessingHash(fieldKey, nextItem, function (addToHashStatus) {
                                            callback('Add item to processing hash ' + addToHashStatus)
                                        });
                                    } else {
                                        RemoveItemFromProcessingHash(fieldKey, function (removeFromHashStatus) {
                                            callback('Remove item from processing hash ' + removeFromHashStatus)
                                        });
                                    }

                                });

                            } else {
                                callback('No session found ,ignore SetNextItem')
                            }

                        } else {
                            callback('Session Mismatched ,ignore SetNextItem')
                        }

                    };

                    var AddNewItem = function (callback) {
                        var fieldKey = util.format('%s:%s', queueId, uuidv4());

                        GetNextItemToProcess(function (nextItem) {

                            if (nextItem) {
                                AddItemToProcessingHash(fieldKey, nextItem, function (addToHashStatus) {
                                    callback('Add item to processing hash ' + addToHashStatus)
                                });
                            } else {
                                callback('No new item found in queue')
                            }

                        });
                    };

                    var RemoveItem = function (callback) {
                        if (processingHashDetail.MatchingValues && processingHashDetail.MatchingValues.indexOf(currentSession) > -1) {

                            var hashRecords = processingHashDetail.MatchingKeyValues.filter(function (item) {
                                return item.Value === currentSession;
                            });

                            if (hashRecords && hashRecords.length > 0) {

                                var fieldKey = hashRecords[0].Field;

                                RemoveItemFromProcessingHash(fieldKey, function (removeFromHashStatus) {
                                    callback('Remove item from processing hash ' + removeFromHashStatus)
                                });

                            } else {
                                callback('No session found ,ignore RemoveItem')
                            }

                        } else {
                            callback('Session Mismatched ,ignore RemoveItem')
                        }
                    };

                    if (exeCount === processingHashDetail.ItemCount) {

                        SetNextItem(function (setNextStatus) {
                            lock.unlock()
                                .catch(function (err) {
                                    logger.error("Release redis lock error:: "+err);
                                });
                            logger.info('SetNextItem process finished :: ' + setNextStatus);
                            callback('done');
                        });

                    } else if (exeCount > processingHashDetail.ItemCount) {

                        SetNextItem(function (setNextStatus) {
                            logger.info('SetNextItem process finished :: ' + setNextStatus);

                            var addCount = exeCount - processingHashDetail.ItemCount;
                            var asyncFuncList = [];
                            for (var i = 0; i < addCount; i++) {
                                asyncFuncList.push(function (callback) {
                                    AddNewItem(function (addNewStatus) {
                                        callback(null, addNewStatus);
                                    })
                                });
                            }

                            if (asyncFuncList.length > 0) {
                                async.series(asyncFuncList, function (err, results) {
                                    logger.info('Add new item process finished :: ' + results);
                                    lock.unlock()
                                        .catch(function (err) {
                                            logger.error("Release redis lock error:: "+err);
                                        });
                                    callback('done');
                                });
                            } else {
                                callback('done');
                            }

                        });

                    } else {
                        RemoveItem(function (removeStatus) {
                            lock.unlock()
                                .catch(function (err) {
                                    logger.error("Release redis lock error:: "+err);
                                });
                            logger.info('SetNextItem process finished :: ' + removeStatus);
                            callback('done');
                        });
                    }

                } else {
                    logger.info("No processing hash detail found, ignore set next item");
                    lock.unlock()
                        .catch(function (err) {
                            logger.error("Release redis lock error:: "+err);
                        });
                    callback("done");
                }

            });

        });
    });
};

var GetRejectedQueueId = function (queueId) {
    //var splitQueueId = queueId.split(":");
    //splitQueueId.splice(-1, 1);
    //return util.format("%s:%s", splitQueueId.join(":"), "REJECTED");
    return util.format("%s:%s", queueId, "REJECTED");
};

var SendQueuePositionInfo = function (logKey, url, queueId, callbackOption, callback) {
    logger.info('%s:Queue: %s ************************* Start SendQueuePositionInfo *************************', logKey, queueId);
    GetProcessingQueueInfo(logKey, queueId, function (err, processingHashItem) {

        var RequestPositionList = [];

        if (processingHashItem) {
            RequestPositionList.push(processingHashItem);

            redisHandler.GetRangeFromList(logKey, queueId, function (err, result) {
                if (err) {
                    logger.error("GetRangeFromList error:: "+err);
                } else {
                    if (result) {
                        result.forEach(function (item, i) {
                            if (item) {
                                var queuePosition = i + 2;
                                var requestPosition = {
                                    SessionId: item,
                                    QueueId: queueId,
                                    QueuePosition: queuePosition.toString()
                                };
                                RequestPositionList.push(requestPosition);

                            }
                        });

                        if (callbackOption == "GET") {
                            restClientHandler.DoGetDirect(url, RequestPositionList, function (err, res, result) {
                                if (err) {
                                    logger.error("DoGetDirect error:: "+err);
                                } else {
                                    logger.info("SendQueuePositionInfo: %s", result);
                                }
                            });
                        } else {
                            restClientHandler.DoPostDirect(url, RequestPositionList, function (err, res, result) {
                                if (err) {
                                    logger.error("DoPostDirect error:: "+err);
                                } else {
                                    logger.info("SendQueuePositionInfo: %s", result);
                                }
                            });
                        }
                    }
                }
            });
        }
    });

    callback("done");
};

var GetProcessingQueueInfo = function (logKey, queueId, callback) {
    logger.info('%s:Queue: %s ************************* Start SendProcessingQueueInfo *************************', logKey, queueId);

    var splitQueueId = queueId.split(":");
    if (splitQueueId && splitQueueId.length > 5) {
        var hashKey = util.format('ProcessingHash:%s:%s:%s', splitQueueId[1], splitQueueId[2], splitQueueId[4]);

        redisHandler.GetHashValue(logKey, hashKey, queueId, function (err, sessionId) {
            if (!err && sessionId) {
                var positionInfo = {SessionId: sessionId, QueueId: queueId, QueuePosition: "1"};
                callback(null, positionInfo);
            } else {
                callback(null, null);
            }
        });

    } else {
        callback(null, null);
    }
};

//var SendProcessingQueueInfo = function(logKey, queueId, sessionId, callback){
//    logger.info('%s:Queue: %s ************************* Start SendProcessingQueueInfo *************************', logKey, queueId);
//
//    var splitQueueId = queueId.split(":");
//    if(splitQueueId && splitQueueId.length > 3) {
//        var reqKey = util.format('Request:%s:%s:%s', splitQueueId[1], splitQueueId[2], sessionId);
//        redisHandler.GetObj(logKey, reqKey, function(err, strObj){
//            if(!err && strObj) {
//                var obj = JSON.parse(strObj);
//                if(obj && obj.QPositionEnable) {
//                    var body = [{SessionId: sessionId, QueueId: queueId, QueuePosition: "1"}];
//                    if (obj.CallbackOption == "GET") {
//                        restClientHandler.DoGetDirect(obj.QPositionUrl, body, function (err, res, result) {
//                            if (err) {
//                                console.log(err);
//                            } else {
//                                console.log("SendQueuePositionInfo: %s", result);
//                            }
//                        });
//                    } else {
//                        restClientHandler.DoPostDirect(url, body, function (err, res, result) {
//                            if (err) {
//                                console.log(err);
//                            } else {
//                                console.log("SendQueuePositionInfo: %s", result);
//                            }
//                        });
//                    }
//                }
//            }
//        });
//
//    }
//
//    callback("done");
//};

module.exports.AddRequestToQueue = AddRequestToQueue;
module.exports.ReAddRequestToQueue = ReAddRequestToQueue;
module.exports.RemoveRequestFromQueue = RemoveRequestFromQueue;
module.exports.GetNextRequestToProcess = GetNextRequestToProcess;
module.exports.SetNextProcessingItem = SetNextProcessingItem;
module.exports.SendQueuePositionInfo = SendQueuePositionInfo;