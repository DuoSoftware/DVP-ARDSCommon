var redis = require('ioredis');
var util = require('util');
var EventEmiter = require('events').EventEmitter;
var config = require('config');
var logger = require("dvp-common/LogHandler/CommonLogHandler.js").logger;
var Redlock = require('redlock');

var redisip = config.Redis.ip;
var redisport = config.Redis.port;
var redispass = config.Redis.password;
var redismode = config.Redis.mode;
var redisdb = config.Redis.db;



var redisSetting =  {
    port:redisport,
    host:redisip,
    family: 4,
    password: redispass,
    db: redisdb,
    retryStrategy: function (times) {
        var delay = Math.min(times * 50, 2000);
        return delay;
    },
    reconnectOnError: function (err) {

        return true;
    }
};

if(redismode == 'sentinel'){

    if(config.Redis.sentinels && config.Redis.sentinels.hosts && config.Redis.sentinels.port, config.Redis.sentinels.name){
        var sentinelHosts = config.Redis.sentinels.hosts.split(',');
        if(Array.isArray(sentinelHosts) && sentinelHosts.length > 2){
            var sentinelConnections = [];

            sentinelHosts.forEach(function(item){

                sentinelConnections.push({host: item, port:config.Redis.sentinels.port})

            });

            redisSetting = {
                sentinels:sentinelConnections,
                name: config.Redis.sentinels.name,
                password: redispass,
                db: redisdb
            }

        }else{

            logger.error("No enough sentinel servers found .........");
        }

    }
}

var client = undefined;

if(redismode != "cluster") {
    client = new redis(redisSetting);
}else{

    var redisHosts = redisip.split(",");
    if(Array.isArray(redisHosts)){


        redisSetting = [];
        redisHosts.forEach(function(item){
            redisSetting.push({
                host: item,
                port: redisport,
                family: 4,
                password: redispass,
                db: redisdb});
        });

        var client = new redis.Cluster([redisSetting]);

    }else{

        client = new redis(redisSetting);
    }


}

client.on("error", function (err) {
    logger.error('Redis connection error :: %s', err);
});

client.on("connect", function (err) {
    //client.select(config.Redis.redisDB, redis.print);
    logger.info("Redis Connect Success");
});


var redlock = new Redlock(
    [client],
    {
        driftFactor: 0.01,

        retryCount:  10000,

        retryDelay:  200

    }
);

redlock.on('clientError', function(err)
{
    //logger.error('[DVP-Common.AcquireLock] - [%s] - REDIS LOCK FAILED', err);

});



//var lock = require("redis-lock")(client);


var SetTags = function (logKey, tagKey, objKey, callback) {
    var tagMeta = util.format('tagMeta:%s', objKey);

    //infoLogger.DetailLogger.log('info', '..................................................');
    logger.info('%s SetTags - objkey: %s :: tagkey: %s :: tagMetakey: %s', logKey, objKey, tagKey, tagMeta);

    client.get(tagMeta, function (err, result) {
        if (err) {
            logger.error('%s SetTags Get tag meta - Error: %s', logKey, err);
            callback(err, null);
        } else {
            logger.info('%s SetTags Get tag meta - Result: %s', logKey, result || "Not found.");
            if (result == null) {
                client.set(tagKey, objKey, function (err, reply) {
                    if (err) {
                        logger.error('%s SetTags Set new tag - Error: %s', logKey, err);
                        callback(err, null);
                    }
                    else {
                        logger.info('%s SetTags Set new tag success. - Result: %s', logKey, reply);

                        client.set(tagMeta, tagKey, function (err, reply) {
                            if (err) {
                                logger.error('%s SetTags Set tag meta - Error: %s', logKey, err);
                                client.del(tagKey, function (err, reply) {
                                });
                                callback(err, reply);
                            }
                            else {
                                logger.info('%s SetTags Set tag meta success.', logKey);
                                callback(err, reply);
                            }
                        });
                    }
                });
            }
            else {
                client.del(result, function (err, reply) {
                    if (err) {
                        logger.error('%s SetTags Delete old tag - Error: %s', logKey, err);
                        callback(err, null)
                    }
                    else if (reply === 1) {
                        logger.info('%s SetTags Delete old tag success.', logKey);

                        client.set(tagKey, objKey, function (err, reply) {
                            if (err) {
                                logger.error('%s SetTags Set new tag - Error: %s', logKey, err);
                            }
                            else {
                                logger.info('%s SetTags Set new tag success. - Result: %s', logKey, reply);

                                client.set(tagMeta, tagKey, function (err, reply) {
                                    if (err) {
                                        logger.error('%s SetTags Set tag meta - Error: %s', logKey, err);

                                        client.del(tagKey, function (err, reply) {
                                        });

                                        callback(err, reply);
                                    }
                                    else {
                                        logger.info('%s SetTags Set tag meta success.', logKey);

                                        callback(err, reply);
                                    }
                                });
                            }
                        });
                    }
                    else {
                        logger.info('%s SetTags Delete old tag failed.', logKey);
                        callback(null, "Failed");
                    }
                });
            }
        }
    });
};

var RemoveTags = function(tags, objKey){
    if (Array.isArray(tags)) {
        var tagMeta = util.format('tagMeta:%s', objKey);
        client.get(tagMeta, function (err, result) {
            if (err) {
                logger.error('RemoveTags failed:: '+ err);
            } else {
                client.del(result, function (err, reply) { });
                client.del(tagMeta, function (err, reply) { });
            }
        });
    }
};

var SetObj = function (logKey, key, obj, callback) {
    redlock.lock('lock:'+key, 500).then(function(lock) {
        logger.info('%s --------------------------------------------------', logKey);
        logger.info('%s SetObj - key: %s :: obj: %s', logKey, key, obj);

        client.set(key, obj, function (error, reply) {
            lock.unlock()
                .catch(function (err) {
                    logger.error("SetObj failed:: "+ err);
                });
            if (error) {
                logger.error('%s SetObj - key: %s :: Error: %s', logKey, key, error);
                callback(error, null);
            }
            else {
                logger.info('%s SetObj - key: %s :: Reply: %s', logKey, key, reply);
                callback(null, reply);
            }
        });

    });
};

var SetObj_NX = function (logKey, key, obj, callback) {
    redlock.lock('lock:'+key, 500).then(function(lock) {
        logger.info('%s --------------------------------------------------', logKey);
        logger.info('%s SetObj_NX - key: %s :: obj: %s', logKey, key, obj);

        client.setnx(key, obj, function (error, reply) {
            lock.unlock()
                .catch(function (err) {
                    logger.error("Release lock failed:: "+ err);
                });
            if (error) {
                logger.error('%s SetObj_NX - key: %s :: Error: %s', logKey, key, error);
                callback(error, null);
            }
            else {
                logger.info('%s SetObj_NX - key: %s :: Reply: %s', logKey, key, reply);
                callback(null, reply);
            }
        });


    });
};

var RemoveObj = function (logKey, key, callback) {
    //var lockKey = util.format('%s', key.split(":").join(""));
    redlock.lock('lock:'+key, 500).then(function(lock) {
        logger.info('%s --------------------------------------------------', logKey);
        logger.info('%s RemoveObj - key: %s', logKey, key);

        client.del(key, function (err, reply) {
            lock.unlock()
                .catch(function (err) {
                    logger.error("Release lock failed:: "+ err);
                });
            if (err) {
                logger.error('%s RemoveObj - key: %s :: Error: %s', logKey, key, err);
            }
            else if (reply === 1) {
                logger.info('%s RemoveObj - key: %s :: Reply: %s', logKey, key, reply);
                callback(null, "true");
            }
            else {
                logger.info('%s RemoveObj - key: %s :: Reply: %s', logKey, key, reply);
                callback(null, "false");
            }
        });


    });
};

var GetObj = function (logKey, key, callback) {
    logger.info('%s --------------------------------------------------', logKey);
    logger.info('%s GetObj - key: %s', logKey, key);

    client.get(key, function (err, result) {
        if (err) {
            logger.error('%s GetObj - key: %s :: Error: %s', logKey, key, err);
            callback(err, null);
        } else {
            logger.info('%s GetObj - key: %s :: Reply: %s', logKey, key, result);
            callback(null, result);
        }
    });
};


var MGetObj = function (logKey, keys, callback) {
    logger.info('%s --------------------------------------------------', logKey);
    logger.info('%s MGetObj - key: %s', logKey, keys);

    client.mget(keys, function (err, result) {
        if (err) {
            logger.error('%s MGetObj - key: %s :: Error: %s', logKey, keys, err);
            callback(err, null);
        } else {
            logger.info('%s MGetObj - key: %s :: Reply: %s', logKey, keys, result);
            callback(null, result);
        }
    });
};


var AddObj_T = function (logKey, key, obj, tags, callback) {
    //var lockKey = util.format('%s', key.split(":").join(""));
    redlock.lock('lock:'+key, 500).then(function(lock) {
        logger.info('%s --------------------------------------------------', logKey);
        logger.info('%s AddObj_T - key: %s :: obj: %s', logKey, key, obj);

        var vid = 1;
        if (Array.isArray(tags)) {
            var tagkey = util.format('tag:%s', tags.join(":"));

            SetTags(logKey,tagkey, key, function (err, reply) {
                if (err) {
                    lock.unlock()
                        .catch(function (err) {
                            logger.error("Release lock failed:: "+ err);
                        });
                    logger.error("Set Tags failed:: "+ err);
                }
                else if (reply === "OK") {
                    client.set(key, obj, function (error, reply) {
                        lock.unlock()
                            .catch(function (err) {
                                logger.error("Release lock failed:: "+ err);
                            });
                        if (error) {
                            logger.error('%s AddObj_T Error - key: %s :: Error: %s', logKey, key, error);
                            client.del(tagkey, function (err, reply) {
                            });
                            client.del(versionkey, function (err, reply) {
                            });
                        }
                        else {
                            logger.info('%s AddObj_T Success - key: %s', logKey, key);
                            callback(null, reply);
                        }
                    });
                }
            });
        }


    });
};

var SetObj_T = function (logKey, key, obj, tags, callback) {
    //var lockKey = util.format('%s', key.split(":").join(""));
    redlock.lock('lock:'+key, 500).then(function(lock) {
        logger.info('%s --------------------------------------------------', logKey);
        logger.info('%s SetObj_T - key: %s :: obj: %s', logKey, key, obj);

        if (Array.isArray(tags)) {
            var tagkey = util.format('tag:%s', tags.join(":"));
            SetTags(logKey, tagkey, key, function (err, reply) {
                if (err) {
                    lock.unlock()
                        .catch(function (err) {
                            logger.error("Release lock failed:: "+ err);
                        });
                    logger.error("SetTags failed:: "+ err);
                    callback(err, null);
                }
                else if (reply === "OK") {
                    client.set(key, obj, function (error, reply) {
                        lock.unlock()
                            .catch(function (err) {
                                logger.error("Release lock failed:: "+ err);
                            });
                        if (error) {
                            logger.error('%s SetObj_T Error - key: %s :: Error: %s', logKey, key, error);
                            callback(error, null);
                        }
                        else {
                            logger.info('%s SetObj_T Success - key: %s', logKey, key);
                            callback(null, reply);
                        }
                    });
                }else{
                    callback(new Error("redis set faied"), null);
                }
            });
        }else{
            callback(new Error("emppty tag array"), null);
        }


    });
};

var RemoveObj_T = function (logKey, key, tags, callback) {
    //var lockKey = util.format('%s', key.split(":").join(""));
    redlock.lock('lock:'+key, 500).then(function(lock) {
        logger.info('%s --------------------------------------------------', logKey);
        logger.info('%s RemoveObj_T - key: %s', logKey, key);

        if (Array.isArray(tags)) {
            var tagMeta = util.format('tagMeta:%s', key);
            client.get(tagMeta, function (err, result) {
                if (err) {
                    logger.error('%s RemoveObj_T Remove tag Error - key: %s :: Error: %s', logKey, key, err);
                } else {
                    client.del(result, function (err, reply) { });
                    client.del(tagMeta, function (err, reply) { });
                    logger.info('%s RemoveObj_T Remove tag success - key: %s :: tagkey: %s', logKey, key, result);
                }
            });

        }

        client.del(key, function (err, reply) {
            lock.unlock()
                .catch(function (err) {
                    logger.error("Release lock failed:: "+ err);
                });
            if (err) {
                logger.error('%s RemoveObj_T Error - key: %s :: Error: %s', logKey, key, err);
            }
            else if (reply === 1) {
                logger.info('%s RemoveObj_T Success - key: %s', logKey, key);
                callback(null, "true");
            }
            else {
                logger.info('%s RemoveObj_T Failed - key: %s', logKey, key);
                callback(null, "false");
            }
        });


    });
};

var GetObjByTagKey_T = function (logKey, tagKeys) {
    var e = new EventEmiter();
    var count = 0;
    for (var i in tagKeys) {
        var val = tagKeys[i];
        //console.log("    " + i + ": " + val);
        client.get(val, function (err, key) {
            logger.info("Key: " + key);

            GetObj(logKey, key, function (err, obj) {
                e.emit('result', err, obj);
                count++;

                logger.info("res", count);

                if (tagKeys.length === count) {
                    logger.info("end", count);
                    e.emit('end');
                }
            });
        });
    }
    return (e);
};

var SearchObj_T = function (logKey, tags, callback) {
    var result = [];
    var searchKey = util.format('tag*%s*', tags.join(":*"));

    client.keys(searchKey, function (err, replies) {
        if (err) {
            callback(err, result);
        } else {
            logger.info(replies.length + " replies:");
            if (replies.length > 0) {
                var gobtk = GetObjByTagKey_T(logKey, replies);

                gobtk.on('result', function (err, obj) {
                    var obj = JSON.parse(obj);
                    result.push(obj);
                });

                gobtk.on('end', function () {
                    callback(null, result);
                });
            } else {
                callback(null, result);
            }
        }
    });
};


var AddObj_V = function (logKey, key, obj, callback) {
    //var lockKey = util.format('%s', key.split(":").join(""));
    redlock.lock('lock:'+key, 500).then(function(lock) {
        logger.info('%s --------------------------------------------------', logKey);
        logger.info('%s AddObj_V - key: %s :: obj: %s', logKey, key, obj);

        var vid = 1;
        var versionkey = util.format('version:%s', key);
        client.set(versionkey, vid, function (err, reply) {
            if (err) {
                logger.error('%s AddObj_V SetVersion Error - key: %s :: Error: %s', logKey, key, err);

                lock.unlock()
                    .catch(function (err) {
                        logger.error("Release lock failed:: "+ err);
                    });
                callback(err, "SetVersion Failed", vid);
            }
            else if (reply === "OK") {
                client.set(key, obj, function (error, reply) {
                    lock.unlock()
                        .catch(function (err) {
                            logger.error("Release lock failed:: "+ err);
                        });
                    if (error) {
                        logger.error(error);
                        client.del(tagkey, function (err, reply) {
                        });
                        client.del(versionkey, function (err, reply) {
                        });

                        logger.error('%s AddObj_V Error - key: %s :: Error: %s', logKey, key, error);
                        callback(error, "AddObj_V Failed", vid);
                    }
                    else {
                        logger.info('%s AddObj_V Success - key: %s', logKey, key);
                        callback(null, reply, vid);
                    }
                });
            }
        });


    });
};

var SetObj_V = function (logKey, key, obj, cvid, callback) {
    //var lockKey = util.format('%s', key.split(":").join(""));
    redlock.lock('lock:'+key, 500).then(function(lock) {
        logger.info('%s --------------------------------------------------', logKey);
        logger.info('%s SetObj_V - key: %s :: obj: %s :: cvid: %s', logKey, key, obj, cvid);

        var versionkey = util.format('version:%s', key);
        client.get(versionkey, function (err, reply) {
            if (err) {
                logger.error('%s SetObj_V GetVersion Error - key: %s :: Error: %s', logKey, key, err);
                lock.unlock()
                    .catch(function (err) {
                        logger.error("Release lock failed:: "+ err);
                    });
            }
            else if (reply === null) {
                lock.unlock()
                    .catch(function (err) {
                        logger.error("Release lock failed:: "+ err);
                    });
                AddObj_V(key, obj, callback);
            }
            else if (reply === cvid) {
                var versionkey = util.format('version:%s', key);
                client.incr(versionkey, function (err, reply) {
                    if (err) {
                        logger.error('%s SetObj_V SetVersion Error - key: %s :: Error: %s', logKey, key, err);
                        lock.unlock()
                            .catch(function (err) {
                                logger.error("Release lock failed:: "+ err);
                            });
                    }
                    else {
                        var vid = reply
                        client.set(key, obj, function (error, reply) {
                            lock.unlock()
                                .catch(function (err) {
                                    logger.error("Release lock failed:: "+ err);
                                });
                            if (error) {
                                logger.error('%s SetObj_V Error - key: %s :: Error: %s', logKey, key, error);
                                callback(error, reply, vid);
                            }
                            else {
                                logger.info('%s SetObj_V Success - key: %s', logKey, key);
                                callback(null, reply, vid);
                            }
                        });
                    }
                });
            }
            else {
                logger.info('%s SetObj_V VERSION_MISMATCHED - key: %s :: cvid: %s', logKey, key, cvid);
                callback(null, "VERSION_MISMATCHED", cvid);
            }
        });


    });
};

var RemoveObj_V = function (logKey, key, callback) {
    //var lockKey = util.format('%s', key.split(":").join(""));
    redlock.lock('lock:'+key, 500).then(function(lock) {
        logger.info('%s --------------------------------------------------', logKey);
        logger.info('%s RemoveObj_V - key: %s', logKey, key);

        var versionkey = util.format('version:%s', key);
        client.del(versionkey, function (err, reply) { });

        client.del(key, function (err, reply) {
            lock.unlock()
                .catch(function (err) {
                    logger.error("Release lock failed:: "+ err);
                });
            if (err) {
                logger.error('%s RemoveObj_V - key: %s :: Error: %s', logKey, key, err);
            }
            else if (reply === 1) {
                logger.info('%s RemoveObj_V - key: %s :: Reply: %s', logKey, key, reply);
                callback(null, "true");
            }
            else {
                logger.info('%s RemoveObj_V - key: %s :: Reply: %s', logKey, key, reply);
                callback(null, "false");
            }
        });


    });
};

var GetObj_V = function (logKey, key, callback) {
    logger.info('%s --------------------------------------------------', logKey);
    logger.info('%s GetObj_V - key: %s', logKey, key);

    client.get(key, function (err, result) {
        if (err) {
            logger.error('GetObj_V - key: %s :: Error: %s', key, err);
            callback(err, null, 0);
        } else {
            var versionkey = util.format('version:%s', key);
            client.get(versionkey, function (err, vresult) {
                if (err) {
                    logger.error(err);
                    callback(err, null, 0);
                } else {
                    logger.info('GetObj_V - key: %s :: Reply: %s :: vid:%s', key, result, vresult);
                    callback(null, result, vresult);
                }
            });
        }
    });
};


var AddObj_V_T = function (logKey, key, obj, tags, callback) {
    //var lockKey = util.format('%s', key.split(":").join(""));
    redlock.lock('lock:'+key, 500).then(function(lock) {
        logger.info('%s --------------------------------------------------', logKey);
        logger.info('%s AddObj_V_T - key: %s :: obj: %s', logKey, key, obj);

        var vid = 1;
        if (Array.isArray(tags)) {
            var tagkey = util.format('tag:%s', tags.join(":"));
            SetTags(logKey, tagkey, key, function (err, reply) {
                if (err) {
                    lock.unlock()
                        .catch(function (err) {
                            logger.error("Release lock failed:: "+ err);
                        });
                    logger.error(error);
                }
                else if (reply === "OK") {
                    var versionkey = util.format('version:%s', key);
                    client.set(versionkey, vid, function (err, reply) {
                        if (err) {
                            logger.error('%s AddObj_V_T SetVersion Error - key: %s :: Error: %s', logKey, key, err);
                            lock.unlock()
                                .catch(function (err) {
                                    logger.error("Release lock failed:: "+ err);
                                });
                            client.del(tagkey, function (err, reply) {
                            });
                        }
                        else if (reply === "OK") {
                            client.set(key, obj, function (error, reply) {
                                lock.unlock()
                                    .catch(function (err) {
                                        logger.error("Release lock failed:: "+ err);
                                    });
                                if (error) {
                                    logger.error('%s AddObj_V_T Error - key: %s :: Error: %s', logKey, key, error);
                                    client.del(tagkey, function (err, reply) {
                                    });
                                    client.del(versionkey, function (err, reply) {
                                    });
                                }
                                else {
                                    logger.info('%s AddObj_V_T Success - key: %s', logKey, key);
                                    callback(null, reply, vid);
                                }
                            });
                        }
                    });
                }
            });
        }


    });
};

var SetObj_V_T = function (logKey, key, obj, tags, cvid, callback) {
    //var lockKey = util.format('%s', key.split(":").join(""));
    redlock.lock('lock:'+key, 500).then(function(lock) {
        logger.info('%s --------------------------------------------------', logKey);
        logger.info('%s SetObj_V_T - key: %s :: obj: %s :: cvid: %s', logKey, key, obj, cvid);

        var versionkey = util.format('version:%s', key);
        client.get(versionkey, function (err, reply) {
            if (err) {
                logger.error('%s SetObj_V_T GetVersion Error - key: %s :: Error: %s', logKey, key, err);
                lock.unlock()
                    .catch(function (err) {
                        logger.error("Release lock failed:: "+ err);
                    });
            }
            else if (reply === null) {
                dlock.unlock()
                    .catch(function (err) {
                        logger.error("Release lock failed:: "+ err);
                    });
                AddObj_V_T(key, obj, tags, callback);
            }
            else if (reply === cvid) {
                if (Array.isArray(tags)) {
                    var tagkey = util.format('tag:%s', tags.join(":"));
                    SetTags(logKey, tagkey, key, function (err, reply) {
                        if (err) {
                            lock.unlock()
                                .catch(function (err) {
                                    logger.error("Release lock failed:: "+ err);
                                });
                        }
                        else if (reply === "OK") {
                            var versionkey = util.format('version:%s', key);
                            client.incr(versionkey, function (err, reply) {
                                if (err) {
                                    logger.error('%s SetObj_V_T SetVersion Error - key: %s :: Error: %s', logKey, key, err);
                                    lock.unlock()
                                        .catch(function (err) {
                                            logger.error("Release lock failed:: "+ err);
                                        });
                                }
                                else {
                                    var vid = reply
                                    client.set(key, obj, function (error, reply) {
                                        lock.unlock()
                                            .catch(function (err) {
                                                logger.error("Release lock failed:: "+ err);
                                            });
                                        if (error) {
                                            logger.error('%s SetObj_V_T Error - key: %s :: Error: %s', logKey, key, error);
                                        }
                                        else {
                                            logger.info('%s SetObj_V_T Success - key: %s', logKey, key);
                                            callback(null, reply, vid);
                                        }
                                    });
                                }
                            });
                        }
                    });
                }
            }
            else {
                logger.info('%s SetObj_V VERSION_MISMATCHED - key: %s :: cvid: %s', logKey, key, cvid);
                lock.unlock()
                    .catch(function (err) {
                        logger.error("Release lock failed:: "+ err);
                    });
                callback(null, "VERSION_MISMATCHED", cvid);
            }
        });


    });
};

var RemoveObj_V_T = function (logKey, key, tags, callback) {
    //var lockKey = util.format('%s', key.split(":").join(""));
    redlock.lock('lock:'+key, 500).then(function(lock) {
        logger.info('%s --------------------------------------------------', logKey);
        logger.info('%s RemoveObj_V_T - key: %s', logKey, key);

        if (Array.isArray(tags)) {
            var tagMeta = util.format('tagMeta:%s', key);
            client.get(tagMeta, function (err, result) {
                if (err) {
                    logger.error("RemoveObj_V_T failed:: "+ err);
                } else {
                    client.del(result, function (err, reply) { });
                    client.del(tagMeta, function (err, reply) { });
                }
            });
        }

        var versionkey = util.format('version:%s', key);
        client.del(versionkey, function (err, reply) { });

        client.del(key, function (err, reply) {
            lock.unlock()
                .catch(function (err) {
                    logger.error("Release lock failed:: "+ err);
                });
            if (err) {
                logger.error('%s RemoveObj_V_T - key: %s :: Error: %s', logKey, key, err);
                callback(err, "false");
            }
            else if (reply === 1) {
                logger.info('%s RemoveObj_V_T - key: %s :: Reply: %s', logKey, key, reply);
                callback(null, "true");
            }
            else {
                logger.info('%s RemoveObj_V_T - key: %s :: Reply: %s', logKey, key, reply);
                callback(null, "false");
            }
        });


    });
};

var GetObjByTagKey_V_T = function (logKey, tagKeys) {
    var e = new EventEmiter();
    var count = 0;
    for (var i in tagKeys) {
        var val = tagKeys[i];
        logger.info("    " + i + ": " + val);
        client.get(val, function (err, key) {
            logger.info("Key: " + key);

            GetObj_V(logKey, key, function (err, obj, vid) {
                e.emit('result', err, obj, vid);
                count++;

                logger.info("res", count);

                if (tagKeys.length === count) {
                    logger.info("end", count);
                    e.emit('end');
                }
            });
        });
    }
    return (e);
};

var SearchObj_V_T = function (logKey, tags, callback) {
    var result = [];
    var searchKey = util.format('tag*%s*', tags.join(":*"));

    client.keys(searchKey, function (err, replies) {
        logger.info(replies.length + " replies:");
        if (replies.length > 0) {
            var gobtk = GetObjByTagKey_V_T(logKey, replies);

            gobtk.on('result', function (err, obj, vid) {
                var obj = { Obj: JSON.parse(obj), Vid: JSON.parse(vid) };
                result.push(obj);
            });

            gobtk.on('end', function () {
                callback(null, result);
            });
        }
        else {
            callback(null, result);
        }

    });
};


var CheckObjExists = function (logKey, key, callback) {
    logger.info('%s --------------------------------------------------', logKey);
    logger.info('%s CheckObjExists - key: %s', logKey, key);

    client.exists(key, function (error, reply) {
        if (error) {
            logger.error('%s CheckObjExists Error - key: %s :: Error: %s', logKey, key, error);
            callback(error, null);
        }
        else {
            logger.info('%s CheckObjExists - key: %s :: Reply: %s', logKey, key, reply);
            callback(null, reply);
        }
    });
};


var AddItemToListR = function (logKey, key, obj, callback) {
    logger.info('%s --------------------------------------------------', logKey);
    logger.info('%s AddItemToListR - key: %s :: obj: %s', logKey, key, obj);

    redlock.lock('lock:'+key, 500).then(function(lock) {
        client.rpush(key, obj, function (error, reply) {
            if (error) {
                lock.unlock()
                    .catch(function (err) {
                        logger.error("Release lock failed:: "+ err);
                    });
                logger.error('%s AddItemToListR Error - key: %s :: Error: %s', logKey, key, error);
                callback(error, null);
            }
            else {
                lock.unlock()
                    .catch(function (err) {
                        logger.error("Release lock failed:: "+ err);
                    });
                logger.info('%s AddItemToListR - key: %s :: Reply: %s', logKey, key, reply);
                callback(null, reply);
            }
        });


    });
};

var AddItemToListL = function (logKey, key, obj, callback) {
    logger.info('%s --------------------------------------------------', logKey);
    logger.info('%s AddItemToListL - key: %s :: obj: %s', logKey, key, obj);

    redlock.lock('lock:'+key, 500).then(function(lock) {
        client.lpush(key, obj, function (error, reply) {
            if (error) {
                lock.unlock()
                    .catch(function (err) {
                        logger.error("Release lock failed:: "+ err);
                    });
                logger.error('%s AddItemToListL Error - key: %s :: Error: %s', logKey, key, error);
                callback(error, null);
            }
            else {
                lock.unlock()
                    .catch(function (err) {
                        logger.error("Release lock failed:: "+ err);
                    });
                logger.info('%s AddItemToListL - key: %s :: Reply: %s', logKey, key, reply);
                callback(null, reply);
            }
        });


    });
};

var GetItemFromList = function (logKey, key, callback) {
    logger.info('%s --------------------------------------------------', logKey);
    logger.info('%s GetItemFromList - key: %s', logKey, key);

    client.lpop(key, function (err, result) {
        if (err) {
            logger.error('%s GetItemFromList Error - key: %s :: Error: %s', logKey, key, err);
            callback(err, null);
        } else {
            logger.info('%s GetItemFromList - key: %s :: Reply: %s', logKey, key, result);
            callback(null, result);
        }
    });
};

var GetLengthOfTheList = function (logKey, key, callback) {
    logger.info('%s --------------------------------------------------', logKey);
    logger.info('%s GetLengthOfTheList - key: %s', logKey, key);

    client.llen(key, function (err, result) {
        if (err) {
            logger.error('%s GetLengthOfTheList Error - key: %s :: Error: %s', logKey, key, err);
            callback(err, null);
        } else {
            logger.info('%s GetLengthOfTheList - key: %s :: Reply: %s', logKey, key, result);
            callback(null, result);
        }
    });
};

var GetRangeFromList = function (logKey, key, callback) {
    logger.info('%s --------------------------------------------------', logKey);
    logger.info('%s GetRangeFromList - key: %s', logKey, key);

    client.lrange(key, 0, -1, function (err, result) {
        if (err) {
            logger.error('%s GetRangeFromList Error - key: %s :: Error: %s', logKey, key, err);
            callback(err, null);
        } else {
            logger.info('%s GetRangeFromList - key: %s :: Reply: %s', logKey, key, result);
            callback(null, result);
        }
    });
};

var RemoveItemFromList = function (logKey, key, obj, callback) {
    logger.info('%s --------------------------------------------------', logKey);
    logger.info('%s RemoveItemFromList - key: %s :: obj: %s', logKey, key, obj);

    redlock.lock('lock:'+key, 500).then(function(lock) {
        client.lrem(key, 0, obj, function (err, result) {
            if (err) {
                lock.unlock()
                    .catch(function (err) {
                        logger.error("Release lock failed:: "+ err);
                    });
                logger.error('%s RemoveItemFromList Error - key: %s :: Error: %s', logKey, key, err);
                callback(err, null);
            } else {
                lock.unlock()
                    .catch(function (err) {
                        logger.error("Release lock failed:: "+ err);
                    });
                logger.info('%s RemoveItemFromList - key: %s :: Reply: %d', logKey, key, result);
                callback(null, result);
            }
        });


    });
};


var AddItemToHash = function (logKey, hashKey, field, obj, callback) {
    logger.info('%s --------------------------------------------------', logKey);
    logger.info('%s AddItemToHash - hashKey: %s :: field: %s :: obj: %s', logKey, hashKey, field, obj);

    redlock.lock('lock:'+hashKey, 500).then(function(lock) {
        client.hset(hashKey, field, obj, function (err, result) {
            if (err) {
                logger.error('%s AddItemToHash Error - hashKey: %s :: field: %s  :: Error: %s', logKey, hashKey, field, err);
                lock.unlock()
                    .catch(function (err) {
                        logger.error("Release lock failed:: "+ err);
                    });
                callback(err, null);
            } else {
                logger.info('%s AddItemToHash - hashKey: %s :: field: %s  :: Reply: %s', logKey, hashKey, field, result);
                lock.unlock()
                    .catch(function (err) {
                        logger.error("Release lock failed:: "+ err);
                    });
                callback(null, result);
            }
        });


    });
};

var RemoveItemFromHash = function (logKey, hashKey, field, callback) {
    logger.info('%s --------------------------------------------------', logKey);
    logger.info('%s RemoveItemFromHash - hashKey: %s :: field: %s', logKey, hashKey, field);

    redlock.lock('lock:'+hashKey, 500).then(function(lock) {
        client.hdel(hashKey, field, function (err, result) {
            if (err) {
                logger.error('%s RemoveItemFromHash Error - hashKey: %s :: field: %s  :: Error: %s', logKey, hashKey, field, err);
                lock.unlock()
                    .catch(function (err) {
                        logger.error("Release lock failed:: "+ err);
                    });
                callback(err, null);
            } else {
                logger.info('%s RemoveItemFromHash - hashKey: %s :: field: %s  :: Reply: %s', logKey, hashKey, field, result);
                lock.unlock()
                    .catch(function (err) {
                        logger.error("Release lock failed:: "+ err);
                    });
                callback(null, result);
            }
        });


    });
};

var RemoveHash = function (logKey, hashKey, callback) {
    logger.info('%s --------------------------------------------------', logKey);
    logger.info('%s RemoveHash - hashKey: %s', logKey, hashKey);
    redlock.lock('lock:'+hashKey, 500).then(function(lock) {
        client.del(hashKey, function (err, result) {
            if (err) {
                logger.error('%s RemoveHash Error - hashKey: %s :: Error: %s', logKey, hashKey, err);
                lock.unlock()
                    .catch(function (err) {
                        logger.error("Release lock failed:: "+ err);
                    });
                callback(err, null);
            } else {
                logger.info('%s RemoveHash - hashKey: %s :: Reply: %s', logKey, hashKey, result);
                lock.unlock()
                    .catch(function (err) {
                        logger.error("Release lock failed:: "+ err);
                    });
                callback(null, result);
            }
        });


    });
};

var CheckHashFieldExists = function (logKey, hashkey, field, callback) {
    logger.info('%s --------------------------------------------------', logKey);
    logger.info('%s CheckHashFieldExists - hashKey: %s :: field: %s', logKey, hashkey, field);
    CheckObjExists(logKey, hashkey, function (err, hresult) {
        if (err) {
            logger.error('%s CheckHashExists Error - hashKey: %s:: Error: %s', logKey, hashkey, err);
            callback(err, null, null);
        }
        else if (hresult == "0") {
            logger.info('%s CheckHashExists - hashKey: %s:: Reply: %s', logKey, hashkey, hresult);
            callback(null, hresult, hresult);
        }else{
            client.hexists(hashkey, field, function (err, result) {
                if (err) {
                    logger.error('%s CheckHashFieldExists Error - hashKey: %s :: field: %s  :: Error: %s', logKey, hashkey, field, err);
                    callback(err, null, null);
                } else {
                    logger.info('%s CheckHashFieldExists - hashKey: %s :: field: %s  :: Reply: %s', logKey, hashkey, field, result);
                    callback(null, hresult, result);
                }
            });
        }
    });
};

var GetHashValue = function(logKey, hashKey, field, callback) {
    logger.info('%s --------------------------------------------------', logKey);
    logger.info('%s GetHashValue - hashKey: %s :: field: %s', logKey, hashKey, field);

    client.hget(hashKey, field, function (err, result) {
        if (err) {
            logger.error('%s GetHashValue Error - hashKey: %s :: field: %s  :: Error: %s', logKey, hashKey, field, err);
            callback(err, null, field);
        } else {
            logger.info('%s GetHashValue - hashKey: %s :: field: %s  :: Reply: %s', logKey, hashKey, field, result);
            callback(null, result, field);
        }
    });
};

var GetAllHashValue = function (logKey, hashkey, callback) {
    logger.info('%s --------------------------------------------------', logKey);
    logger.info('%s GetAllHashValue - hashKey: %s', logKey, hashKey);

    client.hvals(hashKey, function (err, result) {
        if (err) {
            logger.error('%s GetAllHashValue Error - hashKey: %s :: Error: %s', logKey, hashKey, err);
            callback(err, null);
        } else {
            logger.info('%s GetAllHashValue - hashKey: %s :: Reply: %s', logKey, hashKey, result);
            callback(null, result);
        }
    });
};

var AddItemToHashNX = function (logKey, hashKey, field, obj, callback) {
    logger.info('%s --------------------------------------------------', logKey);
    logger.info('%s AddItemToHash - hashKey: %s :: field: %s :: obj: %s', logKey, hashKey, field, obj);

    redlock.lock('lock:'+hashKey, 500).then(function(lock) {
        client.hsetnx(hashKey, field, obj, function (err, result) {
            if (err) {
                logger.error('%s AddItemToHash Error - hashKey: %s :: field: %s  :: Error: %s', logKey, hashKey, field, err);
                lock.unlock()
                    .catch(function (err) {
                        logger.error("Release lock failed:: "+ err);
                    });
                callback(err, null);
            } else {
                logger.info('%s AddItemToHash - hashKey: %s :: field: %s  :: Reply: %s', logKey, hashKey, field, result);
                lock.unlock()
                    .catch(function (err) {
                        logger.error("Release lock failed:: "+ err);
                    });
                callback(null, result);
            }
        });


    });
};

var HScanHash = function (logKey, hashKey, pattern, callback) {
    logger.info('%s --------------------------------------------------', logKey);
    logger.info('%s HScanHash - hashKey: %s :: pattern: %s', logKey, hashKey, pattern);

    var response = {
        MatchingKeyValues: [],
        MatchingValues: [],
        ItemCount: 0
    };
    var stream = client.hscanStream(hashKey, {
        match: pattern
    });

    stream.on('data', function (resultKeys) {
        for (var i = 0; i < resultKeys.length; i+=2) {
            response.MatchingKeyValues.push({Field: resultKeys[i], Value: resultKeys[i+1]});
            response.MatchingValues.push(resultKeys[i+1]);
        }
        response.ItemCount = response.MatchingKeyValues.length;
    });
    stream.on('end', function () {
        callback(response);
    });
};


var Publish = function(logKey, pattern, message, callback){
    logger.info('%s --------------------------------------------------', logKey);
    logger.info('%s Redis Publish - pattern: %s - message: %s', logKey, pattern, message);

    client.publish(pattern, message, function (err, result) {
        if (err) {
            logger.error('%s Redis Publish Error - pattern: %s :: Error: %s', logKey, pattern, err);
            callback(err, null);
        } else {
            logger.info('%s Redis Publish - pattern: %s :: Reply: %s', logKey, pattern, result);
            callback(null, result);
        }
    });
};

module.exports.SetObj = SetObj;
module.exports.SetObj_NX = SetObj_NX;
module.exports.RemoveObj = RemoveObj;
module.exports.GetObj = GetObj;
module.exports.MGetObj = MGetObj;
module.exports.SetTags = SetTags;
module.exports.RemoveTags = RemoveTags;

module.exports.AddObj_T = AddObj_T;
module.exports.SetObj_T = SetObj_T;
module.exports.RemoveObj_T = RemoveObj_T;
module.exports.SearchObj_T = SearchObj_T;

module.exports.AddObj_V = AddObj_V;
module.exports.SetObj_V = SetObj_V;
module.exports.RemoveObj_V = RemoveObj_V;
module.exports.GetObj_V = GetObj_V;

module.exports.AddObj_V_T = AddObj_V_T;
module.exports.SetObj_V_T = SetObj_V_T;
module.exports.RemoveObj_V_T = RemoveObj_V_T;
module.exports.SearchObj_V_T = SearchObj_V_T;

module.exports.CheckObjExists = CheckObjExists;

module.exports.AddItemToListR = AddItemToListR;
module.exports.AddItemToListL = AddItemToListL;
module.exports.GetItemFromList = GetItemFromList;
module.exports.GetLengthOfTheList = GetLengthOfTheList;
module.exports.GetRangeFromList = GetRangeFromList;
module.exports.RemoveItemFromList = RemoveItemFromList;

module.exports.AddItemToHash = AddItemToHash;
module.exports.RemoveItemFromHash = RemoveItemFromHash;
module.exports.CheckHashFieldExists = CheckHashFieldExists;
module.exports.GetHashValue = GetHashValue;
module.exports.GetAllHashValue = GetAllHashValue;
module.exports.RemoveHash = RemoveHash;
module.exports.AddItemToHashNX = AddItemToHashNX;
module.exports.HScanHash = HScanHash;

module.exports.Publish = Publish;

module.exports.RLock = redlock;