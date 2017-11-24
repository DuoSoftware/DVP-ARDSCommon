
var util = require('util');
var q = require('q');
var config = require('config');
var redisHandler = require('./RedisHandler');
var rabbitMqHandler = require('./RabbitMQHandler');

var publishEvent = function (logKey, tenant, company, eventClass, eventType, eventCategory, param1, param2, sessionId, timeStamp) {
    var deferred = q.defer();

    try{

        if(config.Host.UseDashboardMsgQueue === 'true'){

            var eventData = {
                Tenent: tenant,
                Company: company,
                EventClass: eventClass,
                EventType: eventType,
                EventCategory: eventCategory,
                SessionID: sessionId,
                TimeStamp: timeStamp,
                Parameter1: param1,
                Parameter2: param2
            };

            rabbitMqHandler.Publish(logKey, 'DashboardEvents', eventData);

        }else {
            var pubMessage = util.format("EVENT:%d:%d:%s:%s:%s:%s:%s:%s:YYYY", tenant, company, eventClass, eventType, eventCategory, param1, param2, sessionId);
            redisHandler.Publish(logKey, "events", pubMessage, function(){});
        }
        deferred.resolve('ProcessFinished');

    }catch (ex){
        deferred.resolve('ProcessFinished');
    }

    return deferred.promise;
};

module.exports.PublishEvent = publishEvent;