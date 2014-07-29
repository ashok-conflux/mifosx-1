CREATE TABLE `job_error_log` (
	`id` BIGINT(20) NOT NULL AUTO_INCREMENT,
	`job_id` BIGINT(20) NOT NULL,
	`start_time` DATETIME NOT NULL,
	`entity_type_id` SMALLINT(5) NOT NULL,
	`entity_id` BIGINT(20) NOT NULL,
	`error_message` VARCHAR(150) DEFAULT NULL,
	PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;