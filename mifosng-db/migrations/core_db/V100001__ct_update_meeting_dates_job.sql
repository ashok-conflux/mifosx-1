CREATE TABLE `ct_calendar_dates` (
	`id` BIGINT(20) NOT NULL AUTO_INCREMENT,
	`calendar_instance_id` BIGINT(20) NOT NULL,
	`meeting_date` DATE NOT NULL,
	PRIMARY KEY (id),
	FOREIGN KEY (`calendar_instance_id`) REFERENCES m_calendar_instance(id),
	CONSTRAINT uk_EntityDateId UNIQUE (`calendar_instance_id`,`meeting_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO `job` (`name`, `display_name`, `cron_expression`, `create_time`, `task_priority`, `group_name`, `previous_run_start_time`, `next_run_time`, `job_key`, `initializing_errorlog`, `is_active`, `currently_running`, `updates_allowed`, `scheduler_group`) VALUES ('Update Calendar Dates', 'Update Calendar Dates', '0 0 1 1/1 * ? *', now(), 5, NULL, NULL, NULL, NULL, NULL, 1, 0, 1, 0);