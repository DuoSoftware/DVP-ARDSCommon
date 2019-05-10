var rabbitmqHandler = require('./RabbitMQHandler.js');

 var SendDVPEvent = function(resourceId, businessUnit, statusType, status, reason, tenant, company, sessionId) {

     if(statusType === 'ResourceStatus')
     {
         var date = new Date();
         var evtData =
             {
                 SessionId: sessionId,
                 EventName: status,
                 CompanyId: company,
                 TenantId: tenant,
                 EventClass: "AGENT",
                 EventType: "STATUS",
                 EventCategory: status.toUpperCase(),
                 EventTime: date.toISOString(),
                 EventData: "",
                 EventParams: "",
                 EventSpecificData: {
                     EventType: status.toUpperCase(),
                     Reason: reason.toUpperCase(),
                     ResourceId: resourceId,
                     Timestamp: date.valueOf(),
                     BusinessUnit: businessUnit
                 },
                 BusinessUnit: businessUnit
             };

         rabbitmqHandler.Publish('', 'DVPEVENTS', evtData);

     }
 };

module.exports.SendDVPEvent = SendDVPEvent;

