# Environment variables

DVP-ARDSCommon is a shared library. It reads its settings from the config of the service that uses it, by config key (for example `config.DB.Cluster`), not by environment variable name.

- Each service that uses the library maps these config keys to the variables below in its own `config/custom-environment-variables.js`, and lists them in its own `ENVIRONMENT_VARIABLES.md`.
- The library has no `config/` folder of its own.
- **Default** is what the library uses when the service does not set the key. Real hosts, URLs and secrets are never written here.
- Whenever the library starts or stops reading a config key or a variable, or a value's meaning changes, update this file in the same change. See `.claude/rules/environment-variables.md`.

## Config keys read from the service

| Config key | Variable | Description | Type | Possible values | Default | Required | Secret |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `DB.Cluster` | `SYS_POSTGRES_CLUSTER_ENABLED` | dvp-dbmodels: use a connection pool sized by SYS_POSTGRES_POOL_MAX and SYS_POSTGRES_POOL_MIN. | bool | true \| false | off when unset | no | no |
| `DB.Database` | `SYS_POSTGRES_DB` | Postgres database name. | string | database name | — | yes | no |
| `DB.Host` | `SYS_POSTGRES_HOST` | Postgres host. | host | host name or IP | — | yes | no |
| `DB.Password` | `SYS_POSTGRES_PASSWORD` | Postgres password. | string | password | — | yes | yes |
| `DB.PgBouncer` | `SYS_POSTGRES_PGBOUNCER_ENABLED` | dvp-dbmodels: the service connects through PgBouncer, so no statement_timeout is sent and the database role limits apply. | bool | true \| false | false (library default) | no | no |
| `DB.PoolMax` | `SYS_POSTGRES_POOL_MAX` | dvp-dbmodels: most connections in the pool. Used only when SYS_POSTGRES_CLUSTER_ENABLED is true. | int | connections, e.g. 5 | 5 (library default) | no | no |
| `DB.PoolMin` | `SYS_POSTGRES_POOL_MIN` | dvp-dbmodels: fewest connections kept in the pool. Used only when SYS_POSTGRES_CLUSTER_ENABLED is true. | int | connections, e.g. 0 | 0 (library default) | no | no |
| `DB.Port` | `SYS_POSTGRES_PORT` | Postgres port. | port | 1–65535 | — | yes | no |
| `DB.Type` | `SYS_POSTGRES_TYPE` | Sequelize dialect used by dvp-dbmodels. | enum | postgres | — | yes | no |
| `DB.User` | `SYS_POSTGRES_USER` | Postgres user. | string | user name | — | yes | no |
| `Host.LBIP` | `SYS_LB_HOST` | Public host of the load balancer / API gateway; used to build URLs to other services. | host | host name | — | yes | no |
| `Host.LBPort` | `SYS_LB_PORT` | Port of the load balancer. | port | 1–65535 | — | yes | no |
| `Host.UseDashboardMsgQueue` | `SYS_DASHBOARD_MSG_QUEUE_ENABLED` | Send dashboard events through RabbitMQ instead of Redis pub/sub. | bool | true \| false | — | yes | no |
| `Host.Version` | `HOST_VERSION` | API version segment of this service's own routes (/DVP/API/\<version>/...). | version | version string, e.g. 1.0.0.0 | — | yes | no |
| `RabbitMQ.ip` | `SYS_RABBITMQ_HOST` | RabbitMQ host. | host | host name or IP; some services accept a comma-separated list | — | yes | no |
| `RabbitMQ.password` | `SYS_RABBITMQ_PASSWORD` | RabbitMQ password. | string | password | — | yes | yes |
| `RabbitMQ.port` | `SYS_RABBITMQ_PORT` | RabbitMQ port. | port | 1–65535 | — | yes | no |
| `RabbitMQ.user` | `SYS_RABBITMQ_USER` | RabbitMQ user. | string | user name | — | yes | no |
| `RabbitMQ.vhost` | `SYS_RABBITMQ_VHOST` | RabbitMQ virtual host. | string | virtual host, e.g. / | — | yes | no |
| `Redis.db` | `SYS_REDIS_DB` | Main Redis database index. | int | 0–15 | — | yes | no |
| `Redis.ip` | `SYS_REDIS_HOST` | Main Redis host. Also used for token checks (Security block). | host | host name or IP; comma-separated list in cluster mode | — | yes | no |
| `Redis.mode` | `SYS_REDIS_MODE` | How to connect to the main Redis. | enum | instance \| cluster \| sentinel | — | yes | no |
| `Redis.password` | `SYS_REDIS_PASSWORD` | Main Redis password. | string | password | — | yes | yes |
| `Redis.port` | `SYS_REDIS_PORT` | Main Redis port. | port | 1–65535 | — | yes | no |
| `Redis.sentinels.hosts` | `SYS_REDIS_SENTINEL_HOSTS` | Sentinel hosts, when SYS_REDIS_MODE is sentinel. | list | comma-separated host names | — | no | no |
| `Redis.sentinels.name` | `SYS_REDIS_SENTINEL_NAME` | Sentinel master name, when SYS_REDIS_MODE is sentinel. | string | sentinel master name | — | no | no |
| `Redis.sentinels.port` | `SYS_REDIS_SENTINEL_PORT` | Sentinel port, when SYS_REDIS_MODE is sentinel. | port | 1–65535 | — | no | no |
| `Services.accessToken` | `HOST_TOKEN` | Access token this service sends when it calls other services. | string | JWT | — | yes | yes |
| `Services.ardsMonitoringServiceHost` | `SYS_ARDSMONITORING_HOST` | Host other services use to call DVP-ARDSMonitoring. | host | host name or IP, e.g. ardsmonitoring.internal | — | yes | no |
| `Services.ardsMonitoringServicePort` | `SYS_ARDSMONITORING_PORT` | Port of DVP-ARDSMonitoring; added to the URL only when SYS_DYNAMIC_PORT_ENABLED is true. | port | 1–65535 | — | yes | no |
| `Services.ardsMonitoringServiceVersion` | `SYS_ARDSMONITORING_VERSION` | API version used in DVP-ARDSMonitoring URLs. | version | version string, e.g. 1.0.0.0 | — | yes | no |
| `Services.callServerPort` | `SYS_CALLSERVER_PORT` | Port of the call server HTTP API. | port | 1–65535 | — | yes | no |
| `Services.callServerUrl` | `SYS_CALLSERVER_URL` | Base URL of the call server HTTP API. | url | http://host | — | yes | no |
| `Services.cronport` | `SYS_SCHEDULEWORKER_PORT` | Port of DVP-ScheduleWorker; added to the URL only when SYS_DYNAMIC_PORT_ENABLED is true. | port | 1–65535 | — | yes | no |
| `Services.cronurl` | `SYS_SCHEDULEWORKER_HOST` | Host other services use to call DVP-ScheduleWorker. | host | host name or IP, e.g. scheduleworker.internal | — | yes | no |
| `Services.cronversion` | `SYS_SCHEDULEWORKER_VERSION` | API version used in DVP-ScheduleWorker URLs. | version | version string, e.g. 1.0.0.0 | — | yes | no |
| `Services.dynamicPort` | `SYS_DYNAMIC_PORT_ENABLED` | Append the port to service hosts when building service URLs. Turn off when services are reached by host name through a load balancer. | bool | true \| false | — | yes | no |
| `Services.notificationServiceHost` | `SYS_NOTIFICATIONSERVICE_HOST` | Host other services use to call DVP-NotificationService. | host | host name or IP, e.g. notificationservice.internal | — | yes | no |
| `Services.notificationServicePort` | `SYS_NOTIFICATIONSERVICE_PORT` | Port of DVP-NotificationService; added to the URL only when SYS_DYNAMIC_PORT_ENABLED is true. | port | 1–65535 | — | yes | no |
| `Services.notificationServiceVersion` | `SYS_NOTIFICATIONSERVICE_VERSION` | API version used in DVP-NotificationService URLs. | version | version string, e.g. 1.0.0.0 | — | yes | no |
| `Services.resourceServiceHost` | `SYS_RESOURCESERVICE_HOST` | Host other services use to call DVP-ResourceService. | host | host name or IP, e.g. resourceservice.internal | — | yes | no |
| `Services.resourceServicePort` | `SYS_RESOURCESERVICE_PORT` | Port of DVP-ResourceService; added to the URL only when SYS_DYNAMIC_PORT_ENABLED is true. | port | 1–65535 | — | yes | no |
| `Services.resourceServiceVersion` | `SYS_RESOURCESERVICE_VERSION` | API version used in DVP-ResourceService URLs. | version | version string, e.g. 1.0.0.0 | — | yes | no |
| `Services.routingServiceHost` | `SYS_ARDSLITEROUTINGENGINE_HOST` | Host other services use to call DVP-ARDSLiteRoutingEngine. | host | host name or IP, e.g. ardsliteroutingengine.internal | — | yes | no |
| `Services.routingServicePort` | `SYS_ARDSLITEROUTINGENGINE_PORT` | Port of DVP-ARDSLiteRoutingEngine; added to the URL only when SYS_DYNAMIC_PORT_ENABLED is true. | port | 1–65535 | — | yes | no |

## Variables read directly

Read straight from the environment by the library code. These names are reserved and never change.

| Variable | Description | Type | Possible values | Default | Required | Secret |
| --- | --- | --- | --- | --- | --- | --- |
| `DEPLOYMENT_ENV` | docker turns off the log file, so the logger writes to the console only. | string | docker, or unset | unset: console and file | no | no |
| `HOST_NAME` | Host name of the running instance. Also written into every log line by dvp-common. | string | host name | — | yes | no |
| `LOG_LEVEL` | Lowest level the logger writes. | enum | error \| warn \| info \| verbose \| debug \| silly | debug | no | no |
| `LOG_PATH` | Folder for logger.log. Not used when DEPLOYMENT_ENV is docker. | path | folder path, e.g. /var/log/facetone | unset: logger.log in the working folder | no | no |
| `SYS_LOG_HOST` | Logstash host that receives the logs over UDP. Logs are sent only when SYS_LOG_HOST and SYS_LOG_PORT are both set. | host | host name or IP, e.g. logstash.internal | unset: not sent | no | no |
| `SYS_LOG_PORT` | Logstash UDP port. Used with SYS_LOG_HOST. | port | 1–65535 | unset: not sent | no | no |
