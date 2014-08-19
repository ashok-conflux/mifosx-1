/**
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.mifosplatform.scheduledjobs.service;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.joda.time.LocalDate;
import org.mifosplatform.infrastructure.configuration.domain.ConfigurationDomainService;
import org.mifosplatform.infrastructure.core.data.ApiParameterError;
import org.mifosplatform.infrastructure.core.domain.JdbcSupport;
import org.mifosplatform.infrastructure.core.exception.PlatformApiDataValidationException;
import org.mifosplatform.infrastructure.core.service.Page;
import org.mifosplatform.infrastructure.core.service.PaginationHelper;
import org.mifosplatform.infrastructure.core.service.RoutingDataSourceServiceFactory;
import org.mifosplatform.infrastructure.core.service.ThreadLocalContextUtil;
import org.mifosplatform.infrastructure.jobs.annotation.CronTarget;
import org.mifosplatform.infrastructure.jobs.exception.JobExecutionException;
import org.mifosplatform.infrastructure.jobs.service.JobName;
import org.mifosplatform.organisation.holiday.domain.Holiday;
import org.mifosplatform.organisation.holiday.domain.HolidayRepositoryWrapper;
import org.mifosplatform.portfolio.calendar.service.CalendarReadPlatformService;
import org.mifosplatform.portfolio.savings.DepositAccountType;
import org.mifosplatform.portfolio.savings.data.DepositAccountData;
import org.mifosplatform.portfolio.savings.data.SavingsAccountAnnualFeeData;
import org.mifosplatform.portfolio.savings.domain.SavingsAccountCharge;
import org.mifosplatform.portfolio.savings.domain.SavingsAccountChargeRepository;
import org.mifosplatform.portfolio.savings.service.DepositAccountReadPlatformService;
import org.mifosplatform.portfolio.savings.service.DepositAccountWritePlatformService;
import org.mifosplatform.portfolio.savings.service.SavingsAccountChargeReadPlatformService;
import org.mifosplatform.portfolio.savings.service.SavingsAccountWritePlatformService;
import org.mifosplatform.scheduledjobs.data.FutureChargeScheduleInstallment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service(value = "scheduledJobRunnerService")
public class ScheduledJobRunnerServiceImpl implements ScheduledJobRunnerService {

    private final static Logger logger = LoggerFactory.getLogger(ScheduledJobRunnerServiceImpl.class);

    private final RoutingDataSourceServiceFactory dataSourceServiceFactory;
    private final SavingsAccountWritePlatformService savingsAccountWritePlatformService;
    private final SavingsAccountChargeReadPlatformService savingsAccountChargeReadPlatformService;
    private final SavingsAccountChargeRepository savingsAccountChargeRepository;
    private final DepositAccountReadPlatformService depositAccountReadPlatformService;
    private final DepositAccountWritePlatformService depositAccountWritePlatformService;
    private final CalendarReadPlatformService calendarReadPlatformService;
    private final ConfigurationDomainService configurationDomainService;
    private final HolidayRepositoryWrapper holidayRepository;
    private final PaginationHelper<FutureChargeScheduleInstallment> paginationHelper;

    @Autowired
    public ScheduledJobRunnerServiceImpl(final RoutingDataSourceServiceFactory dataSourceServiceFactory,
            final SavingsAccountWritePlatformService savingsAccountWritePlatformService,
            final SavingsAccountChargeReadPlatformService savingsAccountChargeReadPlatformService,
            final SavingsAccountChargeRepository savingsAccountChargeRepository,
            final DepositAccountReadPlatformService depositAccountReadPlatformService,
            final DepositAccountWritePlatformService depositAccountWritePlatformService,
            final ConfigurationDomainService configurationDomainService,
            final HolidayRepositoryWrapper holidayRepository,
            final CalendarReadPlatformService calendarReadPlatformService) {
        this.dataSourceServiceFactory = dataSourceServiceFactory;
        this.savingsAccountWritePlatformService = savingsAccountWritePlatformService;
        this.savingsAccountChargeReadPlatformService = savingsAccountChargeReadPlatformService;
        this.savingsAccountChargeRepository = savingsAccountChargeRepository;
        this.depositAccountReadPlatformService = depositAccountReadPlatformService;
        this.depositAccountWritePlatformService = depositAccountWritePlatformService;
        this.calendarReadPlatformService = calendarReadPlatformService;
        this.configurationDomainService = configurationDomainService;
        this.holidayRepository = holidayRepository;
        this.paginationHelper = new PaginationHelper<>();
    }

    @Transactional
    @Override
    @CronTarget(jobName = JobName.UPDATE_LOAN_SUMMARY)
    public void updateLoanSummaryDetails() {

        final JdbcTemplate jdbcTemplate = new JdbcTemplate(this.dataSourceServiceFactory.determineDataSourceService().retrieveDataSource());

        final StringBuilder updateSqlBuilder = new StringBuilder(900);
        updateSqlBuilder.append("update m_loan ");
        updateSqlBuilder.append("join (");
        updateSqlBuilder.append("SELECT ml.id AS loanId,");
        updateSqlBuilder.append("SUM(mr.principal_amount) as principal_disbursed_derived, ");
        updateSqlBuilder.append("SUM(IFNULL(mr.principal_completed_derived,0)) as principal_repaid_derived, ");
        updateSqlBuilder.append("SUM(IFNULL(mr.principal_writtenoff_derived,0)) as principal_writtenoff_derived,");
        updateSqlBuilder.append("SUM(IFNULL(mr.interest_amount,0)) as interest_charged_derived,");
        updateSqlBuilder.append("SUM(IFNULL(mr.interest_completed_derived,0)) as interest_repaid_derived,");
        updateSqlBuilder.append("SUM(IFNULL(mr.interest_waived_derived,0)) as interest_waived_derived,");
        updateSqlBuilder.append("SUM(IFNULL(mr.interest_writtenoff_derived,0)) as interest_writtenoff_derived,");
        updateSqlBuilder
                .append("SUM(IFNULL(mr.fee_charges_amount,0)) + IFNULL((select SUM(lc.amount) from  m_loan_charge lc where lc.loan_id=ml.id and lc.is_active=1 and lc.charge_id=1),0) as fee_charges_charged_derived,");
        updateSqlBuilder
                .append("SUM(IFNULL(mr.fee_charges_completed_derived,0)) + IFNULL((select SUM(lc.amount_paid_derived) from  m_loan_charge lc where lc.loan_id=ml.id and lc.is_active=1 and lc.charge_id=1),0) as fee_charges_repaid_derived,");
        updateSqlBuilder.append("SUM(IFNULL(mr.fee_charges_waived_derived,0)) as fee_charges_waived_derived,");
        updateSqlBuilder.append("SUM(IFNULL(mr.fee_charges_writtenoff_derived,0)) as fee_charges_writtenoff_derived,");
        updateSqlBuilder.append("SUM(IFNULL(mr.penalty_charges_amount,0)) as penalty_charges_charged_derived,");
        updateSqlBuilder.append("SUM(IFNULL(mr.penalty_charges_completed_derived,0)) as penalty_charges_repaid_derived,");
        updateSqlBuilder.append("SUM(IFNULL(mr.penalty_charges_waived_derived,0)) as penalty_charges_waived_derived,");
        updateSqlBuilder.append("SUM(IFNULL(mr.penalty_charges_writtenoff_derived,0)) as penalty_charges_writtenoff_derived ");
        updateSqlBuilder.append(" FROM m_loan ml ");
        updateSqlBuilder.append("INNER JOIN m_loan_repayment_schedule mr on mr.loan_id = ml.id ");
        updateSqlBuilder.append("WHERE ml.disbursedon_date is not null ");
        updateSqlBuilder.append("GROUP BY ml.id ");
        updateSqlBuilder.append(") x on x.loanId = m_loan.id ");

        updateSqlBuilder.append("SET m_loan.principal_disbursed_derived = x.principal_disbursed_derived,");
        updateSqlBuilder.append("m_loan.principal_repaid_derived = x.principal_repaid_derived,");
        updateSqlBuilder.append("m_loan.principal_writtenoff_derived = x.principal_writtenoff_derived,");
        updateSqlBuilder
                .append("m_loan.principal_outstanding_derived = (x.principal_disbursed_derived - (x.principal_repaid_derived + x.principal_writtenoff_derived)),");
        updateSqlBuilder.append("m_loan.interest_charged_derived = x.interest_charged_derived,");
        updateSqlBuilder.append("m_loan.interest_repaid_derived = x.interest_repaid_derived,");
        updateSqlBuilder.append("m_loan.interest_waived_derived = x.interest_waived_derived,");
        updateSqlBuilder.append("m_loan.interest_writtenoff_derived = x.interest_writtenoff_derived,");
        updateSqlBuilder
                .append("m_loan.interest_outstanding_derived = (x.interest_charged_derived - (x.interest_repaid_derived + x.interest_waived_derived + x.interest_writtenoff_derived)),");
        updateSqlBuilder.append("m_loan.fee_charges_charged_derived = x.fee_charges_charged_derived,");
        updateSqlBuilder.append("m_loan.fee_charges_repaid_derived = x.fee_charges_repaid_derived,");
        updateSqlBuilder.append("m_loan.fee_charges_waived_derived = x.fee_charges_waived_derived,");
        updateSqlBuilder.append("m_loan.fee_charges_writtenoff_derived = x.fee_charges_writtenoff_derived,");
        updateSqlBuilder
                .append("m_loan.fee_charges_outstanding_derived = (x.fee_charges_charged_derived - (x.fee_charges_repaid_derived + x.fee_charges_waived_derived + x.fee_charges_writtenoff_derived)),");
        updateSqlBuilder.append("m_loan.penalty_charges_charged_derived = x.penalty_charges_charged_derived,");
        updateSqlBuilder.append("m_loan.penalty_charges_repaid_derived = x.penalty_charges_repaid_derived,");
        updateSqlBuilder.append("m_loan.penalty_charges_waived_derived = x.penalty_charges_waived_derived,");
        updateSqlBuilder.append("m_loan.penalty_charges_writtenoff_derived = x.penalty_charges_writtenoff_derived,");
        updateSqlBuilder
                .append("m_loan.penalty_charges_outstanding_derived = (x.penalty_charges_charged_derived - (x.penalty_charges_repaid_derived + x.penalty_charges_waived_derived + x.penalty_charges_writtenoff_derived)),");
        updateSqlBuilder
                .append("m_loan.total_expected_repayment_derived = (x.principal_disbursed_derived + x.interest_charged_derived + x.fee_charges_charged_derived + x.penalty_charges_charged_derived),");
        updateSqlBuilder
                .append("m_loan.total_repayment_derived = (x.principal_repaid_derived + x.interest_repaid_derived + x.fee_charges_repaid_derived + x.penalty_charges_repaid_derived),");
        updateSqlBuilder
                .append("m_loan.total_expected_costofloan_derived = (x.interest_charged_derived + x.fee_charges_charged_derived + x.penalty_charges_charged_derived),");
        updateSqlBuilder
                .append("m_loan.total_costofloan_derived = (x.interest_repaid_derived + x.fee_charges_repaid_derived + x.penalty_charges_repaid_derived),");
        updateSqlBuilder
                .append("m_loan.total_waived_derived = (x.interest_waived_derived + x.fee_charges_waived_derived + x.penalty_charges_waived_derived),");
        updateSqlBuilder
                .append("m_loan.total_writtenoff_derived = (x.interest_writtenoff_derived +  x.fee_charges_writtenoff_derived + x.penalty_charges_writtenoff_derived),");
        updateSqlBuilder.append("m_loan.total_outstanding_derived=");
        updateSqlBuilder.append(" (x.principal_disbursed_derived - (x.principal_repaid_derived + x.principal_writtenoff_derived)) + ");
        updateSqlBuilder
                .append(" (x.interest_charged_derived - (x.interest_repaid_derived + x.interest_waived_derived + x.interest_writtenoff_derived)) +");
        updateSqlBuilder
                .append(" (x.fee_charges_charged_derived - (x.fee_charges_repaid_derived + x.fee_charges_waived_derived + x.fee_charges_writtenoff_derived)) +");
        updateSqlBuilder
                .append(" (x.penalty_charges_charged_derived - (x.penalty_charges_repaid_derived + x.penalty_charges_waived_derived + x.penalty_charges_writtenoff_derived))");

        final int result = jdbcTemplate.update(updateSqlBuilder.toString());

        logger.info(ThreadLocalContextUtil.getTenant().getName() + ": Results affected by update: " + result);
    }

    @Transactional
    @Override
    @CronTarget(jobName = JobName.UPDATE_LOAN_ARREARS_AGEING)
    public void updateLoanArrearsAgeingDetails() {

        final JdbcTemplate jdbcTemplate = new JdbcTemplate(this.dataSourceServiceFactory.determineDataSourceService().retrieveDataSource());

        jdbcTemplate.execute("truncate table m_loan_arrears_aging");

        final StringBuilder updateSqlBuilder = new StringBuilder(900);

        updateSqlBuilder
                .append("INSERT INTO m_loan_arrears_aging(`loan_id`,`principal_overdue_derived`,`interest_overdue_derived`,`fee_charges_overdue_derived`,`penalty_charges_overdue_derived`,`total_overdue_derived`,`overdue_since_date_derived`)");
        updateSqlBuilder.append("select ml.id as loanId,");
        updateSqlBuilder
                .append("SUM((ifnull(mr.principal_amount,0) - ifnull(mr.principal_completed_derived, 0))) as principal_overdue_derived,");
        updateSqlBuilder
                .append("SUM((ifnull(mr.interest_amount,0)  - ifnull(mr.interest_completed_derived, 0))) as interest_overdue_derived,");
        updateSqlBuilder
                .append("SUM((ifnull(mr.fee_charges_amount,0)  - ifnull(mr.fee_charges_completed_derived, 0))) as fee_charges_overdue_derived,");
        updateSqlBuilder
                .append("SUM((ifnull(mr.penalty_charges_amount,0)  - ifnull(mr.penalty_charges_completed_derived, 0))) as penalty_charges_overdue_derived,");
        updateSqlBuilder.append("SUM((ifnull(mr.principal_amount,0) - ifnull(mr.principal_completed_derived, 0))) +");
        updateSqlBuilder.append("SUM((ifnull(mr.interest_amount,0)  - ifnull(mr.interest_completed_derived, 0))) +");
        updateSqlBuilder.append("SUM((ifnull(mr.fee_charges_amount,0)  - ifnull(mr.fee_charges_completed_derived, 0))) +");
        updateSqlBuilder
                .append("SUM((ifnull(mr.penalty_charges_amount,0)  - ifnull(mr.penalty_charges_completed_derived, 0))) as total_overdue_derived,");
        updateSqlBuilder.append("MIN(mr.duedate) as overdue_since_date_derived ");
        updateSqlBuilder.append(" FROM m_loan ml ");
        updateSqlBuilder.append(" INNER JOIN m_loan_repayment_schedule mr on mr.loan_id = ml.id ");
        updateSqlBuilder.append(" WHERE ml.loan_status_id = 300 "); // active
        updateSqlBuilder.append(" and mr.completed_derived is false ");
        updateSqlBuilder.append(" and mr.duedate < SUBDATE(CURDATE(),INTERVAL  ifnull(ml.grace_on_arrears_ageing,0) day) ");
        updateSqlBuilder.append(" GROUP BY ml.id");

        final int result = jdbcTemplate.update(updateSqlBuilder.toString());

        logger.info(ThreadLocalContextUtil.getTenant().getName() + ": Results affected by update: " + result);
    }

    @Transactional
    @Override
    @CronTarget(jobName = JobName.UPDATE_LOAN_PAID_IN_ADVANCE)
    public void updateLoanPaidInAdvance() {

        final JdbcTemplate jdbcTemplate = new JdbcTemplate(this.dataSourceServiceFactory.determineDataSourceService().retrieveDataSource());

        jdbcTemplate.execute("truncate table m_loan_paid_in_advance");

        final StringBuilder updateSqlBuilder = new StringBuilder(900);

        updateSqlBuilder
                .append("INSERT INTO m_loan_paid_in_advance(loan_id, principal_in_advance_derived, interest_in_advance_derived, fee_charges_in_advance_derived, penalty_charges_in_advance_derived, total_in_advance_derived)");
        updateSqlBuilder.append(" select ml.id as loanId,");
        updateSqlBuilder.append(" SUM(ifnull(mr.principal_completed_derived, 0)) as principal_in_advance_derived,");
        updateSqlBuilder.append(" SUM(ifnull(mr.interest_completed_derived, 0)) as interest_in_advance_derived,");
        updateSqlBuilder.append(" SUM(ifnull(mr.fee_charges_completed_derived, 0)) as fee_charges_in_advance_derived,");
        updateSqlBuilder.append(" SUM(ifnull(mr.penalty_charges_completed_derived, 0)) as penalty_charges_in_advance_derived,");
        updateSqlBuilder
                .append(" (SUM(ifnull(mr.principal_completed_derived, 0)) + SUM(ifnull(mr.interest_completed_derived, 0)) + SUM(ifnull(mr.fee_charges_completed_derived, 0)) + SUM(ifnull(mr.penalty_charges_completed_derived, 0))) as total_in_advance_derived");
        updateSqlBuilder.append(" FROM m_loan ml ");
        updateSqlBuilder.append(" INNER JOIN m_loan_repayment_schedule mr on mr.loan_id = ml.id ");
        updateSqlBuilder.append(" WHERE ml.loan_status_id = 300 ");
        updateSqlBuilder.append(" and mr.duedate >= CURDATE() ");
        updateSqlBuilder.append(" GROUP BY ml.id");
        updateSqlBuilder
                .append(" HAVING (SUM(ifnull(mr.principal_completed_derived, 0)) + SUM(ifnull(mr.interest_completed_derived, 0)) +");
        updateSqlBuilder
                .append(" SUM(ifnull(mr.fee_charges_completed_derived, 0)) + SUM(ifnull(mr.penalty_charges_completed_derived, 0))) > 0.0");

        final int result = jdbcTemplate.update(updateSqlBuilder.toString());

        logger.info(ThreadLocalContextUtil.getTenant().getName() + ": Results affected by update: " + result);
    }

    @Override
    @CronTarget(jobName = JobName.APPLY_ANNUAL_FEE_FOR_SAVINGS)
    public void applyAnnualFeeForSavings() {

        final Collection<SavingsAccountAnnualFeeData> annualFeeData = this.savingsAccountChargeReadPlatformService
                .retrieveChargesWithAnnualFeeDue();

        for (final SavingsAccountAnnualFeeData savingsAccountReference : annualFeeData) {
            try {
                this.savingsAccountWritePlatformService.applyAnnualFee(savingsAccountReference.getId(),
                        savingsAccountReference.getAccountId());
            } catch (final PlatformApiDataValidationException e) {
                final List<ApiParameterError> errors = e.getErrors();
                for (final ApiParameterError error : errors) {
                    logger.error("Apply annual fee failed for account:" + savingsAccountReference.getAccountNo() + " with message "
                            + error.getDeveloperMessage());
                }
            } catch (final Exception ex) {
                // need to handle this scenario
            }
        }

        logger.info(ThreadLocalContextUtil.getTenant().getName() + ": Savings accounts affected by update: " + annualFeeData.size());
    }

    @Override
    @CronTarget(jobName = JobName.PAY_DUE_SAVINGS_CHARGES)
    public void applyDueChargesForSavings() throws JobExecutionException {
    	
    	updateDueDateAndOutstanding();
    	
        Page<SavingsAccountAnnualFeeData> chargesDueData = this.savingsAccountChargeReadPlatformService
                .retrieveChargesWithDue(0);
        final StringBuilder errorMsg = new StringBuilder();
        
        applyDueChargesForSavingsPage(chargesDueData.getPageItems(), errorMsg);
        
        final int maxPageSize = 500;
        int totalFilteredRecords = chargesDueData.getTotalFilteredRecords();
        int offsetCounter = maxPageSize;
        int processedRecords = maxPageSize;
        
        while(totalFilteredRecords > processedRecords) {
        	chargesDueData = this.savingsAccountChargeReadPlatformService
                    .retrieveChargesWithDue(offsetCounter);
        	applyDueChargesForSavingsPage(chargesDueData.getPageItems(), errorMsg);
        	offsetCounter += 500;
        	processedRecords += 500;
        }

        /*
         * throw exception if any charge payment fails.
         */
        if (errorMsg.length() > 0) { throw new JobExecutionException(errorMsg.toString()); }
    }
    
    private void updateDueDateAndOutstanding() {
    	final List<SavingsAccountCharge> charges = this.savingsAccountChargeRepository.
    			findChargesRequiringUpdate();
    	
    	for(SavingsAccountCharge charge : charges) {
    		if(charge.isRecurringFee())
    			charge.updateToNextDueDate();
    	}
    }
    
    private StringBuilder applyDueChargesForSavingsPage(final List<SavingsAccountAnnualFeeData> chargesDueData,
    		final StringBuilder errorMsg) {
    	
    	for (final SavingsAccountAnnualFeeData savingsAccountReference : chargesDueData) {
            try {
                this.savingsAccountWritePlatformService.applyChargeDue(savingsAccountReference.getId(),
                        savingsAccountReference.getAccountId());
            } catch (final PlatformApiDataValidationException e) {
                final List<ApiParameterError> errors = e.getErrors();
                for (final ApiParameterError error : errors) {
                    logger.error("Apply Charges due for savings failed for account:" + savingsAccountReference.getAccountNo()
                            + " with message " + error.getDeveloperMessage());
                    errorMsg.append("Apply Charges due for savings failed for account:").append(savingsAccountReference.getAccountNo())
                            .append(" with message ").append(error.getDeveloperMessage());
                }
            }
        }

        logger.info(ThreadLocalContextUtil.getTenant().getName() + ": Savings accounts affected by update: " + chargesDueData.size());
        
        return errorMsg;
    }
    
    private static final class FutureChargeScheduleInstallmentMapper implements RowMapper<FutureChargeScheduleInstallment> {

        public String schema() {
            return " cl.office_id, cs.savings_account_charge_id, ca.recurrence, ca.start_date, max(cs.installment) as "
            	 + "last_installment_number, cs.due_amount, count(cs.id) As number_of_future_meetings, max(cs.due_date) " 
            	 + "As last_future_meeting_date from ct_savings_account_charge_schedule cs inner join m_savings_account_charge "
            	 + "c on c.id = cs.savings_account_charge_id and c.is_active = 1 and cs.due_date >= curdate() inner join "
            	 + "m_calendar_instance ci on ci.entity_type_enum = 6 and ci.entity_id = cs.savings_account_charge_id "
            	 + "inner join m_calendar ca on ca.id = ci.calendar_id inner join m_savings_account s on "
            	 + " c.savings_account_id = s.id inner join m_client cl on s.client_id = cl.id and cl.status_enum <> 600 ";
        }

        @Override
        public FutureChargeScheduleInstallment mapRow(final ResultSet rs, @SuppressWarnings("unused") final int rowNum) throws SQLException {

        	final Long officeId = rs.getLong("office_id");
        	final Long savingsAccountChargeId = rs.getLong("savings_account_charge_id");
            final int numberOfFutureMeetings = rs.getInt("number_of_future_meetings");
            final LocalDate fromDate = JdbcSupport.getLocalDate(rs, "last_future_meeting_date");
            final String recurrence = rs.getString("recurrence");
            final LocalDate startDate = JdbcSupport.getLocalDate(rs, "start_date");
            final Long lastInstallmentNumber = rs.getLong("last_installment_number");
            final BigDecimal dueAmount = rs.getBigDecimal("due_amount");

            return new FutureChargeScheduleInstallment(officeId, savingsAccountChargeId, numberOfFutureMeetings, fromDate,
            		lastInstallmentNumber, dueAmount, startDate, recurrence);
        }
    }

    @Override
    @CronTarget(jobName = JobName.UPDATE_CHARGE_INSTALLMENT_DATES)
    public void updateChargeInstallmentDates(){
    	final JdbcTemplate jdbcTemplate = new JdbcTemplate(this.dataSourceServiceFactory.determineDataSourceService().retrieveDataSource());
    	final FutureChargeScheduleInstallmentMapper rm = new FutureChargeScheduleInstallmentMapper();
    	final StringBuilder sqlBuilder = new StringBuilder(200);
    	final int maxPageSize = 500;
    	
    	final boolean isHolidayEnabled = this.configurationDomainService.isRescheduleInstallmentsOnHolidaysEnabled();
    	
        sqlBuilder.append("select SQL_CALC_FOUND_ROWS ");
        sqlBuilder.append(rm.schema());
        sqlBuilder.append(" group by cs.savings_account_charge_id");
        sqlBuilder.append(" limit " + maxPageSize);
    	final String sqlCountRows = "SELECT FOUND_ROWS()";
    	
        Page<FutureChargeScheduleInstallment> futureCalendars = this.paginationHelper.fetchPage(jdbcTemplate,
        		sqlCountRows, sqlBuilder.toString(), new Object[] {}, rm);
        
        insertFutureChargeScheduleInstallments(futureCalendars, isHolidayEnabled, jdbcTemplate);
        
        int totalFilteredRecords = futureCalendars.getTotalFilteredRecords();
        int offsetCounter = maxPageSize;
        int processedRecords = maxPageSize;
        
        sqlBuilder.append(" offset " + offsetCounter);
        while(totalFilteredRecords > processedRecords) {
        	futureCalendars = this.paginationHelper.fetchPage(jdbcTemplate,
            		sqlCountRows, sqlBuilder.toString().replaceFirst("offset.*$", "offset " + offsetCounter), new Object[] {}, rm);
        	insertFutureChargeScheduleInstallments(futureCalendars, isHolidayEnabled, jdbcTemplate);
        	offsetCounter += 500;
        	processedRecords += 500;
        }
    }
    
    @Transactional
    @Override
    @CronTarget(jobName = JobName.UPDATE_NPA)
    public void updateNPA() {

        final JdbcTemplate jdbcTemplate = new JdbcTemplate(this.dataSourceServiceFactory.determineDataSourceService().retrieveDataSource());

        jdbcTemplate.update("UPDATE m_loan ml SET ml.is_npa=0");

        final StringBuilder updateSqlBuilder = new StringBuilder(900);

        updateSqlBuilder.append("UPDATE m_loan as ml,");
        updateSqlBuilder.append(" (select loan.id from m_loan_repayment_schedule mr ");
        updateSqlBuilder
                .append(" INNER JOIN  m_loan loan on mr.loan_id = loan.id INNER JOIN m_product_loan mpl on mpl.id = loan.product_id  ");
        updateSqlBuilder.append("WHERE loan.loan_status_id = 300 and mr.completed_derived is false ");
        updateSqlBuilder
                .append(" and mr.duedate < SUBDATE(CURDATE(),INTERVAL  ifnull(mpl.overdue_days_for_npa,0) day) group by loan.id)  as sl ");
        updateSqlBuilder.append("SET ml.is_npa=1 where ml.id=sl.id");

        final int result = jdbcTemplate.update(updateSqlBuilder.toString());

        logger.info(ThreadLocalContextUtil.getTenant().getName() + ": Results affected by update: " + result);
    }

    @Override
    @CronTarget(jobName = JobName.UPDATE_DEPOSITS_ACCOUNT_MATURITY_DETAILS)
    public void updateMaturityDetailsOfDepositAccounts() {

        final Collection<DepositAccountData> depositAccounts = this.depositAccountReadPlatformService.retrieveForMaturityUpdate();

        for (final DepositAccountData depositAccount : depositAccounts) {
            try {
                final DepositAccountType depositAccountType = DepositAccountType.fromInt(depositAccount.depositType().getId().intValue());
                this.depositAccountWritePlatformService.updateMaturityDetails(depositAccount.id(), depositAccountType);
            } catch (final PlatformApiDataValidationException e) {
                final List<ApiParameterError> errors = e.getErrors();
                for (final ApiParameterError error : errors) {
                    logger.error("Update maturity details failed for account:" + depositAccount.accountNo() + " with message "
                            + error.getDeveloperMessage());
                }
            } catch (final Exception ex) {
                // need to handle this scenario
            }
        }

        logger.info(ThreadLocalContextUtil.getTenant().getName() + ": Deposit accounts affected by update: " + depositAccounts.size());
    }
    
    @Transactional
    private void insertFutureChargeScheduleInstallments(Page<FutureChargeScheduleInstallment> futureCalendars,
    		final boolean isHolidayEnabled, final JdbcTemplate jdbcTemplate) {
    	
    	final int maxAllowedPersistedCalendarDates = 10;
    	
    	List<FutureChargeScheduleInstallment> futureInstallmentsList = futureCalendars.getPageItems();
    	
    	StringBuilder insertSqlBuilder = null;
    	ArrayList<String> insertStatements = new ArrayList<> ();
    	
    	for(FutureChargeScheduleInstallment futureInstallment : futureInstallmentsList) {
    		
    		long lastInstallmentNumber = futureInstallment.getLastInstallmentNumber();
    		BigDecimal dueAmount = futureInstallment.getDueAmount();
    		Long savingsAccountChargeId = futureInstallment.getSavingsAccountChargeId();
    		int numberOfFutureMeetings = futureInstallment.getNumberOfFutureMeetings();
    		
    		if(numberOfFutureMeetings < 10) {
    			
    			List<Holiday> holidays = this.holidayRepository.findByOfficeIdAndGreaterThanDate(futureInstallment.getOfficeId(),
        				futureInstallment.getFromDate().toDate());
    			final int noOfDatesToProduce = maxAllowedPersistedCalendarDates - numberOfFutureMeetings;
    			final Set<LocalDate> remainingRecurringDates = new HashSet<>(this.calendarReadPlatformService
    					.generateRemainingRecurringDates(futureInstallment.getStartDate(), futureInstallment.getFromDate(),
    							futureInstallment.getRecurrence(), noOfDatesToProduce,
    							isHolidayEnabled, holidays));
    			
    			for(LocalDate futureDate : remainingRecurringDates) {
    				insertSqlBuilder = new StringBuilder(170);
    				insertSqlBuilder.append("INSERT INTO `ct_savings_account_charge_schedule`(`savings_account_charge_id`,"
    						+ " `due_amount`, `installment`, `due_date`) values (");
    				insertSqlBuilder.append(savingsAccountChargeId + "," + dueAmount
    						+ "," + lastInstallmentNumber + ",'" + futureDate.toString() + "'");
    				insertSqlBuilder.append(")");
    				
    				insertStatements.add(insertSqlBuilder.toString());
    			}
    		}
    	}
    	
    	if(insertStatements.size() != 0) {
    		String[] statements = new String[insertStatements.size()];
    		jdbcTemplate.batchUpdate(insertStatements.toArray(statements));
		}
    }

}