INSERT INTO `stretchy_report` (`report_name`, `report_type`, `report_subtype`, `report_category`, `report_sql`, `description`, `core_report`, `use_report`) VALUES ('branchProgressReport', 'Pentaho', NULL, NULL, NULL, NULL, 0, 1);
INSERT INTO `stretchy_report` (`report_name`, `report_type`, `report_subtype`, `report_category`, `report_sql`, `description`, `core_report`, `use_report`) VALUES ('CustomCollectionSheet', 'Pentaho', NULL, NULL, NULL, NULL, 0, 1);

INSERT INTO `stretchy_report_parameter` (`report_id`, `parameter_id`, `report_parameter_name`) VALUES (157, 2, 'endDate');
INSERT INTO `stretchy_report_parameter` (`report_id`, `parameter_id`, `report_parameter_name`) VALUES (157, 5, 'selectOffice');
INSERT INTO `stretchy_report_parameter` (`report_id`, `parameter_id`, `report_parameter_name`) VALUES (158, 1, 'meeting_date');
INSERT INTO `stretchy_report_parameter` (`report_id`, `parameter_id`, `report_parameter_name`) VALUES (158, 5, 'Office');
INSERT INTO `stretchy_report_parameter` (`report_id`, `parameter_id`, `report_parameter_name`) VALUES (158, 6, 'loan_officer');
INSERT INTO `stretchy_report_parameter` (`report_id`, `parameter_id`, `report_parameter_name`) VALUES (158, 1008, 'Center');


INSERT INTO `stretchy_parameter` ( `parameter_name`, `parameter_variable`, `parameter_label`, `parameter_displayType`, `parameter_FormatType`, `parameter_default`, `special`, `selectOne`, `selectAll`, `parameter_sql`, `parent_id`) VALUES ('Center', 'Center', 'Center', 'select', 'number', '0', NULL, NULL, NULL, 'select grp.id AS CENTER_ID,grp.display_name AS CENTER_NAME \r\nfrom m_group grp\r\n inner join m_staff stf on grp.staff_id=stf.id \r\nwhere grp.level_id=1 and (stf.id=\'${loanOfficerId}\' or \'-1\'=\'${loanOfficerId}\') order by CENTER_NAME\r\n\r\n', 6);
INSERT INTO `stretchy_parameter` (`parameter_name`, `parameter_variable`, `parameter_label`, `parameter_displayType`, `parameter_FormatType`, `parameter_default`, `special`, `selectOne`, `selectAll`, `parameter_sql`, `parent_id`) VALUES ('CenterOnOffice', 'CenterOnOffice', 'CenterOnOffice', 'select', 'select', 'n/a', NULL, NULL, NULL, NULL, 5);
INSERT INTO `stretchy_parameter` (`parameter_name`, `parameter_variable`, `parameter_label`, `parameter_displayType`, `parameter_FormatType`, `parameter_default`, `special`, `selectOne`, `selectAll`, `parameter_sql`, `parent_id`) VALUES ('Select Branch', 'officeIdAll', 'Branch', 'select', 'number', '0', NULL, NULL, 'Y', 'select id, name\r\nfrom m_office\r\n where id<>1\r\norder by 2', NULL);


