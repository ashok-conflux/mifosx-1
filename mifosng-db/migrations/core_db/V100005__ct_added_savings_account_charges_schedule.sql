CREATE TABLE `ct_savings_account_charge_schedule` (
	`id` BIGINT(20) NOT NULL AUTO_INCREMENT,
	`savings_account_charge_id` BIGINT(20) NOT NULL,
	`due_amount` decimal(19,6) NOT NULL,
	`installment` smallint(5) NOT NULL,
	`paid_amount` decimal(19,6) DEFAULT NULL,
	`waived_amount` decimal(19,6) DEFAULT NULL,
	`due_date` DATE NOT NULL,
	`obligations_met_on_date` DATE NULL DEFAULT NULL,
	`waived` TINYINT(1) NOT NULL DEFAULT '0',
	PRIMARY KEY (id),
	FOREIGN KEY (`savings_account_charge_id`) REFERENCES m_savings_account_charge(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

ALTER TABLE `m_savings_account_charge`
	ADD COLUMN `is_calendar_inherited` tinyint(4) NOT NULL DEFAULT '0';

INSERT INTO `job` (`name`, `display_name`, `cron_expression`, `create_time`, `task_priority`, `group_name`, `previous_run_start_time`, `next_run_time`, `job_key`, `initializing_errorlog`, `is_active`, `currently_running`, `updates_allowed`, `scheduler_group`) VALUES ('Update Charge Installment Dates', 'Update Charge Installment Dates', '0 0 1 1/1 * ? *', now(), 5, NULL, NULL, NULL, NULL, NULL, 1, 0, 1, 0);