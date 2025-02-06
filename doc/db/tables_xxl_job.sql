#
# XXL-JOB
# Copyright (c) 2015-present, xuxueli.

create database if not exists `xxl_job` default character set utf8mb4 collate utf8mb4_unicode_ci;
use `xxl_job`;

set names utf8mb4;

create table `xxl_job_info`
(
    `id`                        int auto_increment,
    `job_group`                 int                              not null comment '执行器主键id',
    `job_desc`                  varchar(255)                     not null comment '任务描述',
    `add_time`                  datetime                         null comment '添加时间',
    `update_time`               datetime                         null comment '更新时间',
    `author`                    varchar(64)                      null comment '作者',
    `alarm_email`               varchar(255)                     null comment '报警邮件',
    `schedule_type`             varchar(50) default 'none'       not null comment '调度类型',
    `schedule_conf`             varchar(128)                     null comment '调度配置，值含义取决于调度类型',
    `executor_handler`          varchar(255)                     null comment '执行器任务handler',
    `executor_param`            varchar(512)                     null comment '执行器任务参数',
    `glue_type`                 varchar(50)                      not null comment 'glue类型',
    `glue_source`               mediumtext                       null comment 'glue源代码',
    `glue_remark`               varchar(128)                     null comment 'glue备注',
    `glue_updatetime`           datetime                         null comment 'glue更新时间',
    `executor_route_strategy`   varchar(50)                      null comment '执行器路由策略',
    `misfire_strategy`          varchar(50) default 'do_nothing' not null comment '调度过期策略',
    `executor_block_strategy`   varchar(50)                      null comment '阻塞处理策略',
    `executor_timeout`          int         default 0            not null comment '任务执行超时时间，单位秒',
    `executor_fail_retry_count` int         default 0            not null comment '失败重试次数',
    `child_jobid`               varchar(255)                     null comment '子任务id，多个逗号分隔',
    `trigger_status`            tinyint     default 0            not null comment '调度状态：0-停止，1-运行',
    `trigger_last_time`         bigint      default 0            not null comment '上次调度时间',
    `trigger_next_time`         bigint      default 0            not null comment '下次调度时间',
    primary key (`id`)
) engine = InnoDB
  default charset = utf8mb4;

create table `xxl_job_log`
(
    `id`                        bigint(20) not null auto_increment,
    `job_group`                 int(11)    not null comment '执行器主键ID',
    `job_id`                    int(11)    not null comment '任务，主键ID',
    `executor_address`          varchar(255)        default null comment '执行器地址，本次执行的地址',
    `executor_handler`          varchar(255)        default null comment '执行器任务handler',
    `executor_param`            varchar(512)        default null comment '执行器任务参数',
    `executor_sharding_param`   varchar(20)         default null comment '执行器任务分片参数，格式如 1/2',
    `executor_fail_retry_count` int(11)    not null default '0' comment '失败重试次数',
    `trigger_time`              datetime            default null comment '调度-时间',
    `trigger_code`              int(11)    not null comment '调度-结果',
    `trigger_msg`               text comment '调度-日志',
    `handle_time`               datetime            default null comment '执行-时间',
    `handle_code`               int(11)    not null comment '执行-状态',
    `handle_msg`                text comment '执行-日志',
    `alarm_status`              tinyint(4) not null default '0' comment '告警状态：0-默认、1-无需告警、2-告警成功、3-告警失败',
    primary key (`id`),
    key `I_trigger_time` (`trigger_time`),
    key `I_handle_code` (`handle_code`)
) engine = InnoDB
  default charset = utf8mb4;

create table `xxl_job_log_report`
(
    `id`            int(11) not null auto_increment,
    `trigger_day`   datetime         default null comment '调度-时间',
    `running_count` int(11) not null default '0' comment '运行中-日志数量',
    `suc_count`     int(11) not null default '0' comment '执行成功-日志数量',
    `fail_count`    int(11) not null default '0' comment '执行失败-日志数量',
    `update_time`   datetime         default null,
    primary key (`id`),
    unique key `i_trigger_day` (`trigger_day`) using btree
) engine = InnoDB
  default charset = utf8mb4;

create table `xxl_job_logglue`
(
    `id`          int(11)      not null auto_increment,
    `job_id`      int(11)      not null comment '任务，主键ID',
    `glue_type`   varchar(50) default null comment 'GLUE类型',
    `glue_source` mediumtext comment 'GLUE源代码',
    `glue_remark` varchar(128) not null comment 'GLUE备注',
    `add_time`    datetime    default null,
    `update_time` datetime    default null,
    primary key (`id`)
) engine = InnoDB
  default charset = utf8mb4;

create table `xxl_job_registry`
(
    `id`             int(11)      not null auto_increment,
    `registry_group` varchar(50)  not null,
    `registry_key`   varchar(255) not null,
    `registry_value` varchar(255) not null,
    `update_time`    datetime default null,
    primary key (`id`),
    key `i_g_k_v` (`registry_group`, `registry_key`, `registry_value`)
) engine = InnoDB
  default charset = utf8mb4;

create table `xxl_job_group`
(
    `id`           int(11)     not null auto_increment,
    `app_name`     varchar(64) not null comment '执行器AppName',
    `title`        varchar(12) not null comment '执行器名称',
    `address_type` tinyint(4)  not null default '0' comment '执行器地址类型：0=自动注册、1=手动录入',
    `address_list` text comment '执行器地址列表，多地址逗号分隔',
    `update_time`  datetime             default null,
    primary key (`id`)
) engine = InnoDB
  default charset = utf8mb4;

create table `xxl_job_user`
(
    `id`         int(11)     not null auto_increment,
    `username`   varchar(50) not null comment '账号',
    `password`   varchar(50) not null comment '密码',
    `role`       tinyint(4)  not null comment '角色：0-普通用户、1-管理员',
    `permission` varchar(255) default null comment '权限：执行器ID列表，多个逗号分割',
    primary key (`id`),
    unique key `i_username` (`username`) using btree
) engine = InnoDB
  default charset = utf8mb4;

create table `xxl_job_lock`
(
    `lock_name` varchar(50) not null comment '锁名称',
    primary key (`lock_name`)
) engine = InnoDB
  default charset = utf8mb4;

insert into `xxl_job_group`(`id`, `app_name`, `title`, `address_type`, `address_list`, `update_time`)
values (1, 'xxl-job-executor-sample', '示例执行器', 0, null, '2018-11-03 22:21:31');

insert into `xxl_job_info`(`id`, `job_group`, `job_desc`, `add_time`, `update_time`, `author`, `alarm_email`,
                           `schedule_type`, `schedule_conf`, `misfire_strategy`, `executor_route_strategy`,
                           `executor_handler`, `executor_param`, `executor_block_strategy`, `executor_timeout`,
                           `executor_fail_retry_count`, `glue_type`, `glue_source`, `glue_remark`, `glue_updatetime`,
                           `child_jobid`)
values (1, 1, '测试任务1', '2018-11-03 22:21:31', '2018-11-03 22:21:31', 'XXL', '', 'CRON', '0 0 0 * * ? *',
        'DO_NOTHING', 'FIRST', 'demoJobHandler', '', 'SERIAL_EXECUTION', 0, 0, 'BEAN', '', 'GLUE代码初始化',
        '2018-11-03 22:21:31', '');

insert into `xxl_job_user`(`id`, `username`, `password`, `role`, `permission`)
values (1, 'admin', 'e10adc3949ba59abbe56e057f20f883e', 1, null);

insert into `xxl_job_lock` (`lock_name`)
values ('schedule_lock');

commit;

