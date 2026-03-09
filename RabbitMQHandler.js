/**
 * Created by Heshan.i on 4/27/2016.
 */
var amqp = require("amqplib");
var config = require("config");
var util = require("util");
var infoLogger = require("dvp-common/LogHandler/CommonLogHandler.js").logger;

var channel = null;
var connection = null;

var amqpIPs = [];
if (config.RabbitMQ.ip) {
  amqpIPs = config.RabbitMQ.ip.split(",");
}

var rabbitMQHost = amqpIPs.length > 0 ? amqpIPs[0].trim() : "localhost";
var amqpUrl = util.format(
  "amqp://%s:%s@%s:%d/%s?heartbeat=10",
  config.RabbitMQ.user,
  config.RabbitMQ.password,
  rabbitMQHost,
  config.RabbitMQ.port,
  encodeURIComponent(config.RabbitMQ.vhost),
);

var connectToRabbitMQ = async function () {
  try {
    connection = await amqp.connect(amqpUrl);
    channel = await connection.createChannel();
    infoLogger.info("Connection with the queue is OK");

    connection.on("error", function (err) {
      infoLogger.error("RabbitMQ connection error: %s", err);
    });

    connection.on("close", function () {
      infoLogger.error("RabbitMQ connection closed, reconnecting...");
      channel = null;
      connection = null;
      setTimeout(connectToRabbitMQ, 1000);
    });
  } catch (err) {
    infoLogger.error("RabbitMQ connection failed: %s, retrying...", err);
    channel = null;
    connection = null;
    setTimeout(connectToRabbitMQ, 1000);
  }
};

connectToRabbitMQ();

var Publish = function (logKey, messageType, sendObj) {
  infoLogger.info(
    "%s --------------------------------------------------",
    logKey,
  );
  infoLogger.info(
    "%s RabbitMQ Publish - queue: %s - message: %s",
    logKey,
    messageType,
    JSON.stringify(sendObj),
  );

  try {
    if (!channel) {
      infoLogger.error(
        "%s RabbitMQ Publish Error - channel not available",
        logKey,
      );
      return;
    }
    channel.sendToQueue(messageType, Buffer.from(JSON.stringify(sendObj)), {
      contentType: "application/json",
    });
    infoLogger.info(
      "%s RabbitMQ Publish Success - queue: %s :: message: %s",
      logKey,
      messageType,
      JSON.stringify(sendObj),
    );
  } catch (exp) {
    infoLogger.error(
      "%s RabbitMQ Publish Error - queue: %s :: message: %s :: Error: %s",
      logKey,
      messageType,
      JSON.stringify(sendObj),
      exp,
    );
  }
};

module.exports.Publish = Publish;
