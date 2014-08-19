ALTER TABLE `m_savings_account_charge`
	ADD COLUMN `start_date` DATE NOT NULL AFTER `charge_time_enum`;