INSERT INTO `c_configuration` (`name`, `enabled`) VALUES ('reschedule-future-installments', 1);
INSERT INTO `c_configuration` (`name`, `enabled`) VALUES ('reschedule-installments-on-holidays', 1);
UPDATE `c_configuration` set `enabled`=1 where `name` = 'reschedule-repayments-on-holidays';
