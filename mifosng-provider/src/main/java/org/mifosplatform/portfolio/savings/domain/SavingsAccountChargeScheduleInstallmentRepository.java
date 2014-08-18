package org.mifosplatform.portfolio.savings.domain;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

public interface SavingsAccountChargeScheduleInstallmentRepository extends 
	JpaRepository<SavingsAccountChargeScheduleInstallment, Long>, JpaSpecificationExecutor<SavingsAccountChargeScheduleInstallment>{
	
//	@Modifying
//	@Transactional
//	@Query("delete from SavingsAccountChargeScheduleInstallment i where i.calendarInstanceId = :calendarInstanceId")
//	Integer deleteInstallmentsByChargeId(@Param("calendarInstanceId") Long calendarInstanceId);

}