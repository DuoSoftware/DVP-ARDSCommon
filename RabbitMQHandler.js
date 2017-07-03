/**
 * Created by Heshan.i on 4/27/2016.
 */
var amqp = require('amqp');
var config = require('config');
var util = require('util');
var infoLogger = require('dvp-common/LogHandler/CommonLogHandler.js').logger;


//var queueHost = util.format('amqp://%s:%s@%s:%d?heartbeat=10', config.RabbitMQ.user, config.RabbitMQ.password, config.RabbitMQ.ip, config.RabbitMQ.port);

if(config.RabbitMQ.ip) {
    config.RabbitMQ.ip = config.RabbitMQ.ip.split(",");
}


var queueConnection = amqp.createConnection({
    host: config.RabbitMQ.ip,
    port: config.RabbitMQ.port,
    login: config.RabbitMQ.user,
    password: config.RabbitMQ.password,
    vhost: config.RabbitMQ.vhost,
    noDelay: true,
    heartbeat:10
}, {
    reconnect: true,
    reconnectBackoffStrategy: 'linear',
    reconnectExponentialLimit: 120000,
    reconnectBackoffTime: 1000
});

queueConnection.on('ready', function () {

    infoLogger.info("Conection with the queue is OK");

});

var Publish = function(logKey, messageType, sendObj){

    infoLogger.info('%s --------------------------------------------------', logKey);
    infoLogger.info('%s RabbitMQ Publish - queue: %s - message: %s', logKey, messageType, sendObj);

    try {
        queueConnection.publish(messageType, sendObj, {
            contentType: 'application/json'
        });
        infoLogger.info('%s RabbitMQ Publish Success - queue: %s :: message: %s', logKey, messageType, sendObj);
    }catch(exp){

        infoLogger.error('%s RabbitMQ Publish Error - queue: %s :: message: %s :: Error: %s', logKey, messageType, sendObj, exp);
    }
};

/*var Publish = function(logKey, queue, message){
 try {
 infoLogger.DetailLogger.log('info', '%s --------------------------------------------------', logKey);
 infoLogger.DetailLogger.log('info', '%s RabbitMQ Publish - queue: %s - message: %s', logKey, queue, message);
 var url = util.format('amqp://%s:%s@%s:%d/', config.RabbitMQ.user, config.RabbitMQ.password, config.RabbitMQ.ip, config.RabbitMQ.port);
 amqp.connect(url).then(function (conn) {
 return when(conn.createChannel().then(function (ch) {
 var q = queue;
 var msg = message;

 var ok = ch.assertQueue(q, {durable: true});

 return ok.then(function (_qok) {
 // NB: `sentToQueue` and `publish` both return a boolean
 // indicating whether it's OK to send again straight away, or
 // (when `false`) that you should wait for the event `'drain'`
 // to fire before writing again. We're just doing the one write,
 // so we'll ignore it.
 ch.sendToQueue(q, new Buffer(msg));
 console.log(" [x] Sent '%s'", msg);
 infoLogger.DetailLogger.log('info', '%s RabbitMQ Publish Success - queue: %s :: message: %s', logKey, q, msg);
 return ch.close();
 });
 })).ensure(function () {
 conn.close();
 });
 }).then(null, console.warn);
 } catch (ex2) {
 infoLogger.DetailLogger.log('error', '%s RabbitMQ Publish Error - queue: %s :: message: %s :: Error: %s', logKey, queue, message, ex2);
 }
 };*/

module.exports.Publish = Publish;
