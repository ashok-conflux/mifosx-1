INSERT INTO `stretchy_report` (`report_name`, `report_type`, `report_subtype`, `report_category`, `report_sql`, `description`, `core_report`, `use_report`) VALUES ('Due Vs Collected For Loans', 'Pentaho', NULL, NULL, NULL, NULL, 0, 1);

INSERT INTO `stretchy_report_parameter` (`report_id`, `parameter_id`, `report_parameter_name`) VALUES ((select sr.id From stretchy_report sr where sr.report_name='Due Vs Collected For Loans'), (select sp.id from stretchy_parameter sp where sp.parameter_name='OfficeIdSelectOne'), 'Branch');
INSERT INTO `stretchy_report_parameter` (`report_id`, `parameter_id`, `report_parameter_name`) VALUES ((select sr.id From stretchy_report sr where sr.report_name='Due Vs Collected For Loans'),(select sp.id from stretchy_parameter sp where sp.parameter_name='endDateSelect'), 'date');

