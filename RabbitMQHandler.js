/**
 * Created by Heshan.i on 4/27/2016.
 */
var amqp = require('amqplib');
var when = require('when');
var config = require('config');
var util = require('util');
var infoLogger = require('./InformationLogger.js');

var Publish = function(logKey, queue, message){
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
};

module.exports.Publish = Publish;