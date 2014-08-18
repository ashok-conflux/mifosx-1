/**
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.mifosplatform.portfolio.calendar.service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.mifosplatform.infrastructure.configuration.domain.ConfigurationDomainService;
import org.mifosplatform.infrastructure.core.api.JsonCommand;
import org.mifosplatform.infrastructure.core.data.ApiParameterError;
import org.mifosplatform.infrastructure.core.data.CommandProcessingResult;
import org.mifosplatform.infrastructure.core.data.CommandProcessingResultBuilder;
import org.mifosplatform.infrastructure.core.data.DataValidatorBuilder;
import org.mifosplatform.infrastructure.core.domain.JdbcSupport;
import org.mifosplatform.infrastructure.core.exception.PlatformApiDataValidationException;
import org.mifosplatform.infrastructure.core.service.DateUtils;
import org.mifosplatform.infrastructure.core.service.Page;
import org.mifosplatform.infrastructure.core.service.PaginationHelper;
import org.mifosplatform.infrastructure.core.service.RoutingDataSource;
import org.mifosplatform.infrastructure.jobs.annotation.CronTarget;
import org.mifosplatform.infrastructure.jobs.service.JobName;
import org.mifosplatform.organisation.holiday.domain.Holiday;
import org.mifosplatform.organisation.holiday.domain.HolidayRepositoryWrapper;
import org.mifosplatform.organisation.office.domain.Office;
import org.mifosplatform.organisation.workingdays.domain.WorkingDays;
import org.mifosplatform.organisation.workingdays.domain.WorkingDaysRepositoryWrapper;
import org.mifosplatform.portfolio.calendar.CalendarConstants.CALENDAR_SUPPORTED_PARAMETERS;
import org.mifosplatform.portfolio.calendar.data.FutureCalendarData;
import org.mifosplatform.portfolio.calendar.domain.Calendar;
import org.mifosplatform.portfolio.calendar.domain.CalendarDate;
import org.mifosplatform.portfolio.calendar.domain.CalendarDateRepository;
import org.mifosplatform.portfolio.calendar.domain.CalendarEntityType;
import org.mifosplatform.portfolio.calendar.domain.CalendarHistory;
import org.mifosplatform.portfolio.calendar.domain.CalendarHistoryRepository;
import org.mifosplatform.portfolio.calendar.domain.CalendarInstance;
import org.mifosplatform.portfolio.calendar.domain.CalendarInstanceRepository;
import org.mifosplatform.portfolio.calendar.domain.CalendarRepository;
import org.mifosplatform.portfolio.calendar.domain.CalendarType;
import org.mifosplatform.portfolio.calendar.exception.CalendarNotFoundException;
import org.mifosplatform.portfolio.calendar.serialization.CalendarCommandFromApiJsonDeserializer;
import org.mifosplatform.portfolio.client.domain.Client;
import org.mifosplatform.portfolio.client.domain.ClientRepositoryWrapper;
import org.mifosplatform.portfolio.group.domain.Group;
import org.mifosplatform.portfolio.group.domain.GroupRepositoryWrapper;
import org.mifosplatform.portfolio.loanaccount.domain.Loan;
import org.mifosplatform.portfolio.loanaccount.domain.LoanRepository;
import org.mifosplatform.portfolio.loanaccount.service.LoanWritePlatformService;
import org.mifosplatform.portfolio.savings.domain.SavingsAccountCharge;
import org.mifosplatform.portfolio.savings.domain.SavingsAccountChargeRepository;
import org.mifosplatform.portfolio.savings.service.DepositApplicationProcessWritePlatformService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

@Service
public class CalendarWritePlatformServiceJpaRepositoryImpl implements CalendarWritePlatformService {

    private final CalendarRepository calendarRepository;
    private final CalendarHistoryRepository calendarHistoryRepository;
    private final CalendarDateRepository calendarDateRepository;
    private final CalendarCommandFromApiJsonDeserializer fromApiJsonDeserializer;
    private final CalendarInstanceRepository calendarInstanceRepository;
    private final CalendarReadPlatformService calendarReadPlatformService;
    private final LoanWritePlatformService loanWritePlatformService;
    private final DepositApplicationProcessWritePlatformService depositApplicationProcessWritePlatformService;
    private final ConfigurationDomainService configurationDomainService;
    private final HolidayRepositoryWrapper holidayRepository;
    private final WorkingDaysRepositoryWrapper workingDaysRepository;
    private final GroupRepositoryWrapper groupRepository;
    private final LoanRepository loanRepository;
    private final ClientRepositoryWrapper clientRepository;
    private final SavingsAccountChargeRepository savingsAccountChargeRepository;
    private final JdbcTemplate jdbcTemplate;
    private final PaginationHelper<FutureCalendarData> paginationHelper;

    @Autowired
    public CalendarWritePlatformServiceJpaRepositoryImpl(final CalendarRepository calendarRepository,
            final CalendarHistoryRepository calendarHistoryRepository,
            final CalendarDateRepository calendarDateRepository,
            final CalendarCommandFromApiJsonDeserializer fromApiJsonDeserializer,
            final CalendarInstanceRepository calendarInstanceRepository, final CalendarReadPlatformService calendarReadPlatformService,
            final LoanWritePlatformService loanWritePlatformService,
            final DepositApplicationProcessWritePlatformService depositApplicationProcessWritePlatformService,
            final HolidayRepositoryWrapper holidayRepository,
            final ConfigurationDomainService configurationDomainService,
            final WorkingDaysRepositoryWrapper workingDaysRepository,
            final GroupRepositoryWrapper groupRepository, final LoanRepository loanRepository,
            final SavingsAccountChargeRepository savingsAccountChargeRepository,
            final ClientRepositoryWrapper clientRepository, final RoutingDataSource dataSource) {
        this.calendarRepository = calendarRepository;
        this.calendarHistoryRepository = calendarHistoryRepository;
        this.calendarDateRepository = calendarDateRepository;
        this.calendarReadPlatformService = calendarReadPlatformService;
        this.fromApiJsonDeserializer = fromApiJsonDeserializer;
        this.calendarInstanceRepository = calendarInstanceRepository;
        this.loanWritePlatformService = loanWritePlatformService;
        this.depositApplicationProcessWritePlatformService = depositApplicationProcessWritePlatformService;
        this.holidayRepository = holidayRepository;
        this.configurationDomainService = configurationDomainService;
        this.workingDaysRepository = workingDaysRepository;
        this.groupRepository = groupRepository;
        this.loanRepository = loanRepository;
        this.clientRepository = clientRepository;
        this.savingsAccountChargeRepository = savingsAccountChargeRepository;
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.paginationHelper = new PaginationHelper<>();
    }

    @Override
    public CommandProcessingResult createCalendar(final JsonCommand command) {

        this.fromApiJsonDeserializer.validateForCreate(command.json());
        Long entityId = null;
        CalendarEntityType entityType = CalendarEntityType.INVALID;
        LocalDate entityActivationDate = null;
        Group centerOrGroup = null;
        if (command.getGroupId() != null) {
            centerOrGroup = this.groupRepository.findOneWithNotFoundDetection(command.getGroupId());
            entityActivationDate = centerOrGroup.getActivationLocalDate();
            entityType = centerOrGroup.isCenter() ? CalendarEntityType.CENTERS : CalendarEntityType.GROUPS;
            entityId = command.getGroupId();
        } else if (command.getLoanId() != null) {
            final Loan loan = this.loanRepository.findOne(command.getLoanId());
            entityActivationDate = (loan.getApprovedOnDate() == null) ? loan.getSubmittedOnDate() : loan.getApprovedOnDate();
            entityType = CalendarEntityType.LOANS;
            entityId = command.getLoanId();
        } else if (command.getClientId() != null) {
            final Client client = this.clientRepository.findOneWithNotFoundDetection(command.getClientId());
            entityActivationDate = client.getActivationLocalDate();
            entityType = CalendarEntityType.CLIENTS;
            entityId = command.getClientId();
        }
        
        final Integer entityTypeId = entityType.getValue();
        final Calendar newCalendar = Calendar.fromJson(command);

        final List<ApiParameterError> dataValidationErrors = new ArrayList<>();
        final DataValidatorBuilder baseDataValidator = new DataValidatorBuilder(dataValidationErrors).resource("calendar");
        if (entityActivationDate == null || newCalendar.getStartDateLocalDate().isBefore(entityActivationDate)) {
            final DateTimeFormatter formatter = DateTimeFormat.forPattern(command.dateFormat()).withLocale(command.extractLocale());
            String dateAsString = "";
            if (entityActivationDate != null) dateAsString = formatter.print(entityActivationDate);

            final String errorMessage = "cannot.be.before." + entityType.name().toLowerCase() + ".activation.date";
            baseDataValidator.reset().parameter(CALENDAR_SUPPORTED_PARAMETERS.START_DATE.getValue()).value(dateAsString)
                    .failWithCodeNoParameterAddedToErrorCode(errorMessage);
        }

        if (centerOrGroup != null) {
            Long centerOrGroupId = centerOrGroup.getId();
            Integer centerOrGroupEntityTypeId = entityType.getValue();

            final Group parent = centerOrGroup.getParent();
            if (parent != null) {
                centerOrGroupId = parent.getId();
                centerOrGroupEntityTypeId = CalendarEntityType.CENTERS.getValue();
            }

            final CalendarInstance collectionCalendarInstance = this.calendarInstanceRepository
                    .findByEntityIdAndEntityTypeIdAndCalendarTypeId(centerOrGroupId, centerOrGroupEntityTypeId,
                            CalendarType.COLLECTION.getValue());
            if (collectionCalendarInstance != null) {
                final String errorMessage = "multiple.collection.calendar.not.supported";
                baseDataValidator.reset().failWithCodeNoParameterAddedToErrorCode(errorMessage);
            }
        }

        if (!dataValidationErrors.isEmpty()) { throw new PlatformApiDataValidationException(dataValidationErrors); }

        this.calendarRepository.save(newCalendar);

        final CalendarInstance newCalendarInstance = CalendarInstance.from(newCalendar, entityId, entityTypeId);
        this.calendarInstanceRepository.save(newCalendarInstance);

        return new CommandProcessingResultBuilder() //
                .withCommandId(command.commandId()) //
                .withEntityId(newCalendar.getId()) //
                .withClientId(command.getClientId()) //
                .withGroupId(command.getGroupId()) //
                .withLoanId(command.getLoanId()) //
                .build();

    }

    @Override
    public CommandProcessingResult updateCalendar(final JsonCommand command) {

        this.fromApiJsonDeserializer.validateForUpdate(command.json());

        final Long calendarId = command.entityId();
        final Calendar calendarForUpdate = this.calendarRepository.findOne(calendarId);
        if (calendarForUpdate == null) { throw new CalendarNotFoundException(calendarId); }
        final Date oldStartDate = calendarForUpdate.getStartDate();
        final LocalDate currentDate = DateUtils.getLocalDateOfTenant();
        // create calendar history before updating calendar
        final CalendarHistory calendarHistory = new CalendarHistory(calendarForUpdate, oldStartDate);
        final Map<String, Object> changes = calendarForUpdate.update(command);

        if (!changes.isEmpty()) {
            // update calendar history table only if there is a change in
            // calendar start date.
            if (currentDate.isAfter(new LocalDate(oldStartDate))) {
                final Date endDate = calendarForUpdate.getStartDateLocalDate().minusDays(1).toDate();
                calendarHistory.updateEndDate(endDate);
                this.calendarHistoryRepository.save(calendarHistory);
            }
            
            this.calendarRepository.saveAndFlush(calendarForUpdate);
            
            deleteCalendarDates(command);
            
            if(calendarForUpdate.isRepeating())
            	updateChargeInstallmentDates(command);
            
            if (this.configurationDomainService.isRescheduleFutureRepaymentsEnabled() && calendarForUpdate.isRepeating()) {
                // fetch all loan calendar instances associated with modifying
                // calendar.
                final Collection<CalendarInstance> loanCalendarInstances = this.calendarInstanceRepository.findByCalendarIdAndEntityTypeId(
                        calendarId, CalendarEntityType.LOANS.getValue());

                if (!CollectionUtils.isEmpty(loanCalendarInstances)) {
                    // update all loans associated with modifying calendar
                    this.loanWritePlatformService.applyMeetingDateChanges(calendarForUpdate, loanCalendarInstances);
                }
            }
            
            if (this.configurationDomainService.isRescheduleFutureInstallmentsEnabled() && calendarForUpdate.isRepeating()) {
                // fetch all savings calendar instances associated with modifying calendar
                final Collection<CalendarInstance> savingsCalendarInstances = this.calendarInstanceRepository.findByCalendarIdAndEntityTypeId(
                        calendarId, CalendarEntityType.SAVINGS.getValue());

                if (!CollectionUtils.isEmpty(savingsCalendarInstances)) {
                    // update all savings associated with modifying calendar
                    this.depositApplicationProcessWritePlatformService.applyRecurringDepositScheduleChanges(calendarForUpdate, savingsCalendarInstances);
                }
            }
        }

        return new CommandProcessingResultBuilder() //
                .withCommandId(command.commandId()) //
                .withEntityId(calendarForUpdate.getId()) //
                .with(changes) //
                .build();
    }

    @Override
    public CommandProcessingResult deleteCalendar(final Long calendarId) {
        final Calendar calendarForDelete = this.calendarRepository.findOne(calendarId);
        if (calendarForDelete == null) { throw new CalendarNotFoundException(calendarId); }

        this.calendarRepository.delete(calendarForDelete);
        return new CommandProcessingResultBuilder() //
                .withCommandId(null) //
                .withEntityId(calendarId) //
                .build();
    }

    @Override
    public CommandProcessingResult createCalendarInstance(final Long calendarId, final Long entityId, final Integer entityTypeId) {

        final Calendar calendarForUpdate = this.calendarRepository.findOne(calendarId);
        if (calendarForUpdate == null) { throw new CalendarNotFoundException(calendarId); }

        final CalendarInstance newCalendarInstance = new CalendarInstance(calendarForUpdate, entityId, entityTypeId);
        this.calendarInstanceRepository.save(newCalendarInstance);

        return new CommandProcessingResultBuilder() //
                .withCommandId(null) //
                .withEntityId(calendarForUpdate.getId()) //
                .build();
    }

    @Override
    public CommandProcessingResult updateCalendarInstance(final Long calendarId, final Long entityId, final Integer entityTypeId) {
        final Calendar calendarForUpdate = this.calendarRepository.findOne(calendarId);
        if (calendarForUpdate == null) { throw new CalendarNotFoundException(calendarId); }

        final CalendarInstance calendarInstanceForUpdate = this.calendarInstanceRepository.findByCalendarIdAndEntityIdAndEntityTypeId(
                calendarId, entityId, entityTypeId);
        this.calendarInstanceRepository.saveAndFlush(calendarInstanceForUpdate);
        return new CommandProcessingResultBuilder() //
                .withCommandId(null) //
                .withEntityId(calendarForUpdate.getId()) //
                .build();
    }
    
    private static final class FutureCenterCalendarMapper implements RowMapper<FutureCalendarData> {

        public String schema() {
            return "g.office_id as office_id, c.recurrence as recurrence, c.start_date as start_date, "
            + "ci.id as calendar_instance_id, count(ccd.id) As number_of_future_meetings, max(ccd.meeting_date) "
            + "As last_future_meeting_date from m_calendar_instance ci inner join m_group g on g.id=ci.entity_id "
            + "and ci.entity_type_enum = 4 inner join m_calendar c on c.id = ci.calendar_id left join ct_calendar_dates ccd "
            + "on ccd.calendar_instance_id = ci.id and ccd.meeting_date >= curdate()";
        }

        @Override
        public FutureCalendarData mapRow(final ResultSet rs, @SuppressWarnings("unused") final int rowNum) throws SQLException {

        	final Long officeId = rs.getLong("office_id");
            final Long calendarInstanceId = rs.getLong("calendar_instance_id");
            final int numberOfFutureMeetings = rs.getInt("number_of_future_meetings");
            final LocalDate fromDate = JdbcSupport.getLocalDate(rs, "last_future_meeting_date");
            final String recurrence = rs.getString("recurrence");
            final LocalDate startDate = JdbcSupport.getLocalDate(rs, "start_date");

            return new FutureCalendarData(officeId, numberOfFutureMeetings, fromDate, calendarInstanceId, startDate, recurrence);
        }
    }
    
    @Override
    @CronTarget(jobName = JobName.UPDATE_CALENDAR_DATES)
    public void updateCalendarDates() {
    	
    	final FutureCenterCalendarMapper rm = new FutureCenterCalendarMapper();
    	final StringBuilder sqlBuilder = new StringBuilder(200);
    	final int maxPageSize = 500;
    	
    	final boolean isHolidayEnabled = this.configurationDomainService.isRescheduleInstallmentsOnHolidaysEnabled();
    	
        sqlBuilder.append("select SQL_CALC_FOUND_ROWS ");
        sqlBuilder.append(rm.schema());
        sqlBuilder.append(" where g.status_enum=300");
        sqlBuilder.append(" group by ci.id");
        sqlBuilder.append(" limit " + maxPageSize);
    	final String sqlCountRows = "SELECT FOUND_ROWS()";
    	
        Page<FutureCalendarData> futureCalendars = this.paginationHelper.fetchPage(this.jdbcTemplate,
        		sqlCountRows, sqlBuilder.toString(), new Object[] {}, rm);
        
        insertCalendarDates(futureCalendars, isHolidayEnabled);
        
        int totalFilteredRecords = futureCalendars.getTotalFilteredRecords();
        int offsetCounter = maxPageSize;
        int processedRecords = maxPageSize;
        
        sqlBuilder.append(" offset " + offsetCounter);
        while(totalFilteredRecords > processedRecords) {
        	futureCalendars = this.paginationHelper.fetchPage(this.jdbcTemplate,
            		sqlCountRows, sqlBuilder.toString().replaceFirst("offset.*$", "offset " + offsetCounter), new Object[] {}, rm);
        	insertCalendarDates(futureCalendars, isHolidayEnabled);
        	offsetCounter += 500;
        	processedRecords += 500;
        }
    }
    
    @Transactional
    private void insertCalendarDates(Page<FutureCalendarData> futureCalendars, final boolean isHolidayEnabled) {
    	
    	final int maxAllowedPersistedCalendarDates = 10;
    	
    	List<FutureCalendarData> futureCalendarsList = futureCalendars.getPageItems();
    	
    	int flushCounter = 0;
    	for(FutureCalendarData futureCalendar : futureCalendarsList) {
    		
    		List<Holiday> holidays = this.holidayRepository.findByOfficeIdAndGreaterThanDate(futureCalendar.getOfficeId(),
    				futureCalendar.getStartDate().toDate());
    		flushCounter++;
    		int numberOfFutureCalendars = futureCalendar.getNumberOfFutureCalendars();
    		
    		if(numberOfFutureCalendars < 10) {
    			final int noOfDatesToProduce = maxAllowedPersistedCalendarDates - futureCalendar.getNumberOfFutureCalendars();
    			final Set<LocalDate> remainingRecurringDates = new HashSet<>(this.calendarReadPlatformService
    					.generateRemainingRecurringDates(futureCalendar.getStartDate(), futureCalendar.getFromDate(),
    							futureCalendar.getRecurrence(), noOfDatesToProduce,
    							isHolidayEnabled, holidays));
    			
    			CalendarDate calendarDate = null;
    			for(LocalDate futureDate : remainingRecurringDates) {
    				calendarDate = new CalendarDate(futureDate, futureCalendar.getCalendarInstanceId());
    				this.calendarDateRepository.save(calendarDate);
    			}
    		}
    		
    		if(flushCounter % 100 == 0) 
				this.calendarDateRepository.flush();
    	}
    }
    
    //Job repopulates dates at end of day.
    private void deleteCalendarDates(final JsonCommand command) {
    	
    	if (command.getGroupId() != null) {
            final Group group = this.groupRepository.findOneWithNotFoundDetection(command.getGroupId());
            
            EntityDetails entityDetails = getCalendarEntityTypeAndEntityId(group);
            Long entityId = entityDetails.getEntityId();
            CalendarEntityType entityType = entityDetails.getEntityType();
            
	        if(entityType.isCenter()) {
	            final CalendarInstance calendarInstance = this.calendarInstanceRepository.findByCalendarIdAndEntityIdAndEntityTypeId(
	                    command.entityId(), entityId, entityType.getValue());
	            this.calendarDateRepository.deleteCalendarDateByCalendarInstanceId(calendarInstance.getId());
	        }
        }
    }
    
	private void updateChargeInstallmentDates(final JsonCommand command) {
	    
	    final List<Long> savingsAccountChargeIds = this.calendarInstanceRepository.
	    		getSavingsAccountChargeIds(command.entityId());
	    if(savingsAccountChargeIds != null) {
	    	
	    	final Group group = this.groupRepository.findOneWithNotFoundDetection(command.getGroupId());
	    	final Office office = group.getOffice();
	    	
	    	EntityDetails entityDetails = getCalendarEntityTypeAndEntityId(group);
            Long entityId = entityDetails.getEntityId();
            CalendarEntityType entityType = entityDetails.getEntityType();
            
            final CalendarInstance calendarInstance = this.calendarInstanceRepository.findByCalendarIdAndEntityIdAndEntityTypeId(
                    command.entityId(), entityId, entityType.getValue());
            final Calendar calendar = calendarInstance.getCalendar();
	    	
	    	final boolean isHolidayEnabled = this.configurationDomainService.isRescheduleInstallmentsOnHolidaysEnabled();
			List<Holiday> holidays = this.holidayRepository.findByOfficeIdAndGreaterThanDate(office.getId(),
					office.getOpeningLocalDate().toDate());
			final WorkingDays workingDays = this.workingDaysRepository.findOne();
			
		    final List<SavingsAccountCharge> savingsAccountCharges = this.savingsAccountChargeRepository
		    		.findAll(savingsAccountChargeIds);
		    for(SavingsAccountCharge savingsAccountCharge : savingsAccountCharges) {
		    	savingsAccountCharge.updateSchedule(calendar, isHolidayEnabled, holidays, workingDays);
		    }
		    
		    this.savingsAccountChargeRepository.save(savingsAccountCharges);
	    }
	}
	
	private EntityDetails getCalendarEntityTypeAndEntityId(final Group group) {
		
		Long entityId = group.getId();
        CalendarEntityType entityType = CalendarEntityType.GROUPS;
        
        /*
         * If group is within a center then center entityType should be
         * passed for retrieving CalendarInstance.
         */
        if (group.isCenter()) {
            entityType = CalendarEntityType.CENTERS;
        } else if (group.isChildGroup()) {
            entityType = CalendarEntityType.CENTERS;
            entityId = group.getParent().getId();
        }
		
		return new EntityDetails(entityId, entityType);
	}
	
	private static final class EntityDetails {
		
		final Long entityId;
		final CalendarEntityType entityType;
		
		public EntityDetails(Long entityId, CalendarEntityType entityType) {
			this.entityId = entityId;
			this.entityType = entityType;
		}
		
		public Long getEntityId() {
			return this.entityId;
		}
		
		public CalendarEntityType getEntityType() {
			return this.entityType;
		}
	}
}
