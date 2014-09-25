/**
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.mifosplatform.portfolio.savings.domain;

import static org.mifosplatform.portfolio.savings.SavingsApiConstants.SAVINGS_ACCOUNT_RESOURCE_NAME;
import static org.mifosplatform.portfolio.savings.SavingsApiConstants.amountParamName;
import static org.mifosplatform.portfolio.savings.SavingsApiConstants.chargeCalculationTypeParamName;
import static org.mifosplatform.portfolio.savings.SavingsApiConstants.chargeIdParamName;
import static org.mifosplatform.portfolio.savings.SavingsApiConstants.chargeTimeTypeParamName;
import static org.mifosplatform.portfolio.savings.SavingsApiConstants.chargesParamName;
import static org.mifosplatform.portfolio.savings.SavingsApiConstants.dueAsOfDateParamName;
import static org.mifosplatform.portfolio.savings.SavingsApiConstants.feeIntervalParamName;
import static org.mifosplatform.portfolio.savings.SavingsApiConstants.feeOnMonthDayParamName;
import static org.mifosplatform.portfolio.savings.SavingsApiConstants.idParamName;
import static org.mifosplatform.portfolio.savings.SavingsApiConstants.calendarInheritedParamName;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.joda.time.LocalDate;
import org.joda.time.MonthDay;
import org.mifosplatform.infrastructure.configuration.domain.ConfigurationDomainService;
import org.mifosplatform.infrastructure.core.data.ApiParameterError;
import org.mifosplatform.infrastructure.core.data.DataValidatorBuilder;
import org.mifosplatform.infrastructure.core.exception.GeneralPlatformDomainRuleException;
import org.mifosplatform.infrastructure.core.exception.PlatformApiDataValidationException;
import org.mifosplatform.infrastructure.core.serialization.FromJsonHelper;
import org.mifosplatform.organisation.holiday.domain.Holiday;
import org.mifosplatform.organisation.holiday.domain.HolidayRepositoryWrapper;
import org.mifosplatform.organisation.workingdays.domain.WorkingDays;
import org.mifosplatform.organisation.workingdays.domain.WorkingDaysRepositoryWrapper;
import org.mifosplatform.portfolio.calendar.domain.Calendar;
import org.mifosplatform.portfolio.calendar.domain.CalendarEntityType;
import org.mifosplatform.portfolio.calendar.domain.CalendarFrequencyType;
import org.mifosplatform.portfolio.calendar.domain.CalendarInstance;
import org.mifosplatform.portfolio.calendar.domain.CalendarInstanceRepository;
import org.mifosplatform.portfolio.calendar.domain.CalendarType;
import org.mifosplatform.portfolio.calendar.service.CalendarUtils;
import org.mifosplatform.portfolio.charge.domain.Charge;
import org.mifosplatform.portfolio.charge.domain.ChargeCalculationType;
import org.mifosplatform.portfolio.charge.domain.ChargeRepositoryWrapper;
import org.mifosplatform.portfolio.charge.domain.ChargeTimeType;
import org.mifosplatform.portfolio.charge.exception.ChargeCannotBeAppliedToException;
import org.mifosplatform.portfolio.charge.exception.SavingsAccountChargeNotFoundException;
import org.mifosplatform.portfolio.group.domain.Group;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

@Service
public class SavingsAccountChargeAssembler {

    private final FromJsonHelper fromApiJsonHelper;
    private final ChargeRepositoryWrapper chargeRepository;
    private final SavingsAccountChargeRepository savingsAccountChargeRepository;
    private final CalendarInstanceRepository calendarInstanceRepository;
    private final HolidayRepositoryWrapper holidayRepository;
    private final ConfigurationDomainService configurationDomainService;
    private final WorkingDaysRepositoryWrapper workingDaysRepository;

    @Autowired
    public SavingsAccountChargeAssembler(final FromJsonHelper fromApiJsonHelper, final ChargeRepositoryWrapper chargeRepository,
            final SavingsAccountChargeRepository savingsAccountChargeRepository,final HolidayRepositoryWrapper holidayRepository,
            final ConfigurationDomainService configurationDomainService, final WorkingDaysRepositoryWrapper workingDaysRepository,
            final CalendarInstanceRepository calendarInstanceRepository) {
        this.fromApiJsonHelper = fromApiJsonHelper;
        this.chargeRepository = chargeRepository;
        this.savingsAccountChargeRepository = savingsAccountChargeRepository;
        this.holidayRepository = holidayRepository;
        this.configurationDomainService = configurationDomainService;
        this.workingDaysRepository = workingDaysRepository;
        this.calendarInstanceRepository = calendarInstanceRepository;
    }

    public Set<SavingsAccountCharge> fromParsedJson(final JsonElement element, final String productCurrencyCode) {

        final Set<SavingsAccountCharge> savingsAccountCharges = new HashSet<>();

        if (element.isJsonObject()) {
            final JsonObject topLevelJsonElement = element.getAsJsonObject();
            final String dateFormat = this.fromApiJsonHelper.extractDateFormatParameter(topLevelJsonElement);
            final String monthDayFormat = this.fromApiJsonHelper.extractMonthDayFormatParameter(topLevelJsonElement);
            final Locale locale = this.fromApiJsonHelper.extractLocaleParameter(topLevelJsonElement);
            if (topLevelJsonElement.has(chargesParamName) && topLevelJsonElement.get(chargesParamName).isJsonArray()) {
                final JsonArray array = topLevelJsonElement.get(chargesParamName).getAsJsonArray();
                for (int i = 0; i < array.size(); i++) {

                    final JsonObject savingsChargeElement = array.get(i).getAsJsonObject();

                    final Long id = this.fromApiJsonHelper.extractLongNamed(idParamName, savingsChargeElement);
                    final Long chargeId = this.fromApiJsonHelper.extractLongNamed(chargeIdParamName, savingsChargeElement);
                    final BigDecimal amount = this.fromApiJsonHelper.extractBigDecimalNamed(amountParamName, savingsChargeElement, locale);
                    final Integer chargeTimeType = this.fromApiJsonHelper.extractIntegerNamed(chargeTimeTypeParamName,
                            savingsChargeElement, locale);
                    final Integer chargeCalculationType = this.fromApiJsonHelper.extractIntegerNamed(chargeCalculationTypeParamName,
                            savingsChargeElement, locale);
                    final LocalDate dueDate = this.fromApiJsonHelper.extractLocalDateNamed(dueAsOfDateParamName, savingsChargeElement,
                            dateFormat, locale);

                    final MonthDay feeOnMonthDay = this.fromApiJsonHelper.extractMonthDayNamed(feeOnMonthDayParamName,
                            savingsChargeElement, monthDayFormat, locale);
                    final Integer feeInterval = this.fromApiJsonHelper.extractIntegerNamed(feeIntervalParamName, savingsChargeElement,
                            locale);
                    
                    final Boolean isCalendarInherited = this.fromApiJsonHelper.extractBooleanNamed(calendarInheritedParamName,
                    		savingsChargeElement);

                    if (id == null) {
                        final Charge chargeDefinition = this.chargeRepository.findOneWithNotFoundDetection(chargeId);

                        if (!chargeDefinition.isSavingsCharge()) {
                            final String errorMessage = "Charge with identifier " + chargeDefinition.getId()
                                    + " cannot be applied to Savings product.";
                            throw new ChargeCannotBeAppliedToException("savings.product", errorMessage, chargeDefinition.getId());
                        }

                        ChargeTimeType chargeTime = null;
                        if (chargeTimeType != null) {
                            chargeTime = ChargeTimeType.fromInt(chargeTimeType);
                        }

                        ChargeCalculationType chargeCalculation = null;
                        if (chargeCalculationType != null) {
                            chargeCalculation = ChargeCalculationType.fromInt(chargeCalculationType);
                        }

                        final boolean status = true;
                        final SavingsAccountCharge savingsAccountCharge = SavingsAccountCharge.createNewWithoutSavingsAccount(
                                chargeDefinition, amount, chargeTime, chargeCalculation, dueDate, status, feeOnMonthDay, feeInterval,
                                isCalendarInherited);
                        savingsAccountCharges.add(savingsAccountCharge);
                    } else {
                        final Long savingsAccountChargeId = id;
                        final SavingsAccountCharge savingsAccountCharge = this.savingsAccountChargeRepository
                                .findOne(savingsAccountChargeId);
                        if (savingsAccountCharge == null) { throw new SavingsAccountChargeNotFoundException(savingsAccountChargeId); }

                        savingsAccountCharge.update(amount, dueDate, feeOnMonthDay, feeInterval, isCalendarInherited);

                        savingsAccountCharges.add(savingsAccountCharge);
                    }
                }
            }
        }

        this.validateSavingsCharges(savingsAccountCharges, productCurrencyCode);
        return savingsAccountCharges;
    }

    public Set<SavingsAccountCharge> fromSavingsProduct(final SavingsProduct savingsProduct, final LocalDate activationDate) {

        final Set<SavingsAccountCharge> savingsAccountCharges = new HashSet<>();
        Set<Charge> productCharges = savingsProduct.charges();
        for (Charge charge : productCharges) {
            ChargeTimeType chargeTime = null;
            if (charge.getChargeTime() != null) {
                chargeTime = ChargeTimeType.fromInt(charge.getChargeTime());
            }
            if (chargeTime != null && chargeTime.isOnSpecifiedDueDate()) {
                continue;
            }

            ChargeCalculationType chargeCalculation = null;
            if (charge.getChargeCalculation() != null) {
                chargeCalculation = ChargeCalculationType.fromInt(charge.getChargeCalculation());
            }
            final boolean status = true;
            final boolean isCalendarInherited = false;
            LocalDate dueDate = activationDate;
            
            final SavingsAccountCharge savingsAccountCharge = SavingsAccountCharge.createNewWithoutSavingsAccount(charge,
                    charge.getAmount(), chargeTime, chargeCalculation, dueDate, status, charge.getFeeOnMonthDay(),
                    charge.feeInterval(), isCalendarInherited);
            savingsAccountCharges.add(savingsAccountCharge);
        }
        return savingsAccountCharges;
    }

    private void validateSavingsCharges(final Set<SavingsAccountCharge> charges, final String productCurrencyCode) {
        final List<ApiParameterError> dataValidationErrors = new ArrayList<>();
        final DataValidatorBuilder baseDataValidator = new DataValidatorBuilder(dataValidationErrors)
                .resource(SAVINGS_ACCOUNT_RESOURCE_NAME);
        boolean isOneWithdrawalPresent = false;
        boolean isOneAnnualPresent = false;
        for (SavingsAccountCharge charge : charges) {
            if (!charge.hasCurrencyCodeOf(productCurrencyCode)) {
                baseDataValidator.reset().parameter("currency").value(charge.getCharge().getId())
                        .failWithCodeNoParameterAddedToErrorCode("currency.and.charge.currency.not.same");
            }

            if (charge.isWithdrawalFee()) {
                if (isOneWithdrawalPresent) {
                    baseDataValidator.reset().failWithCodeNoParameterAddedToErrorCode("multiple.withdrawal.fee.per.account.not.supported");
                }
                isOneWithdrawalPresent = true;
            }

            if (charge.isAnnualFee()) {
                if (isOneAnnualPresent) {
                    baseDataValidator.reset().failWithCodeNoParameterAddedToErrorCode("multiple.annual.fee.per.account.not.supported");
                }
                isOneAnnualPresent = true;
            }
        }
        if (!dataValidationErrors.isEmpty()) { throw new PlatformApiDataValidationException(dataValidationErrors); }
    }
    
    public void generateScheduleForCharges(final SavingsAccount account, boolean isModify) {
    	final boolean isHolidayEnabled = this.configurationDomainService.isRescheduleRepaymentsOnHolidaysEnabled();
	 	final WorkingDays workingDays = this.workingDaysRepository.findOne();
	 	List<Holiday> holidays = this.holidayRepository.findByOfficeIdAndGreaterThanDate(
	 			account.officeId(),	account.accountSubmittedOrActivationDate().toDate());
        
        final Set<SavingsAccountCharge> savingsAccountCharges = account.charges();
        for(final SavingsAccountCharge savingsAccountCharge : savingsAccountCharges) {
        	if(savingsAccountCharge.isRecurringFee() && savingsAccountCharge.getSavingsAccountChargeScheduleInstallments().isEmpty())
        		generateScheduleForCharge(account, savingsAccountCharge, isModify, isHolidayEnabled, holidays, workingDays);
        	
        }
    }
    
    public void generateScheduleForCharge(final SavingsAccount account, final SavingsAccountCharge savingsAccountCharge,
    		boolean isModify, final boolean isHolidayEnabled, final List<Holiday> holidays, final WorkingDays workingDays) {
    	if(savingsAccountCharge.isRecurringFee()) {
    		CalendarInstance calendarInstance = null;
    		if(isModify) {
    			calendarInstance = this.calendarInstanceRepository.findByEntityIdAndEntityTypeIdAndCalendarTypeId(
    					savingsAccountCharge.getId(), CalendarEntityType.SAVINGS_CHARGES.getValue(),
    					CalendarType.COLLECTION.getValue());
    			Calendar calendar = calendarInstance.getCalendar();
    			if (!savingsAccountCharge.isCalendarInherited()) {
			            final LocalDate calendarStartDate = savingsAccountCharge.getApplicableDueDate();
			            final ChargeTimeType chargeTimeType = savingsAccountCharge.getChargeTimeType();
			            final Integer frequency = savingsAccountCharge.getFeeInterval();
			            final Integer repeatsOnDay = calendarStartDate.getDayOfWeek();
	
			            calendar.updateRepeatingCalendar(calendarStartDate, CalendarFrequencyType.from(chargeTimeType), frequency,
			                    repeatsOnDay);
			            this.calendarInstanceRepository.save(calendarInstance);
    		      }
    			
    		} else {
    		calendarInstance = getCalendarInstance(account, savingsAccountCharge);
    		this.calendarInstanceRepository.save(calendarInstance);
    		}
    		final Calendar calendar = calendarInstance.getCalendar();
    		savingsAccountCharge.generateSchedule(calendar, isHolidayEnabled, holidays, workingDays);
    	}
    }
    
    public CalendarInstance getCalendarInstance(SavingsAccount account,
    		SavingsAccountCharge savingsAccountCharge) {
        CalendarInstance calendarInstance = null;
	        if (savingsAccountCharge.isCalendarInherited()) {
	            Set<Group> groups = account.getClient().getGroups();
	            Long groupId = null;
	            if (groups.isEmpty()) {
	                final String defaultUserMessage = "Client does not belong to group/center. Cannot follow group/center meeting frequency.";
	                throw new GeneralPlatformDomainRuleException(
	                        "error.msg.recurring.charge.cannot.create.not.belongs.to.any.groups.to.follow.meeting.frequency",
	                        defaultUserMessage, account.clientId());
	            } else if (groups.size() > 1) {
	                final String defaultUserMessage = "Client belongs to more than one group. Cannot support recurring charge.";
	                throw new GeneralPlatformDomainRuleException(
	                        "error.msg.recurring.charge.cannot.create.belongs.to.multiple.groups", defaultUserMessage,
	                        account.clientId());
	            } else {
	                Group group = groups.iterator().next();
	                Group parent = group.getParent();
	                Integer entityType = CalendarEntityType.GROUPS.getValue();
	                if (parent != null) {
	                    groupId = parent.getId();
	                    entityType = CalendarEntityType.CENTERS.getValue();
	                } else {
	                    groupId = group.getId();
	                }
	                CalendarInstance parentCalendarInstance = this.calendarInstanceRepository.findByEntityIdAndEntityTypeIdAndCalendarTypeId(
	                        groupId, entityType, CalendarType.COLLECTION.getValue());
	                
	                //Validating center/group meeting frequency with charge frequency
	                Calendar parentCalendar = parentCalendarInstance.getCalendar();
	                final CalendarFrequencyType frequency = CalendarUtils.getFrequency(parentCalendar.getRecurrence());
	                Integer recurringEvery = CalendarUtils.getInterval(parentCalendar.getRecurrence());
	                recurringEvery = recurringEvery == -1 ? 1 : recurringEvery;
	                if((savingsAccountCharge.isWeeklyFee() && !frequency.isWeekly()) || 
	                   (savingsAccountCharge.isMonthlyFee() && !frequency.isMonthly()) ||
	                   (savingsAccountCharge.isAnnualFee() && !frequency.isAnnual()) ||
	                   (savingsAccountCharge.getFeeInterval() != recurringEvery)) {
	                	final String defaultUserMessage = "Center/Group meeting frequency does not match charge frequency."
	                			+ " Cannot support recurring charge.";
		                throw new GeneralPlatformDomainRuleException(
		                        "error.msg.recurring.charge.cannot.create.meeting.frequency.and.charge.frequency.not.equal", defaultUserMessage,
		                        account.clientId());
	                }
	                
	                calendarInstance = CalendarInstance.from(parentCalendar, savingsAccountCharge.getId(),
	                        CalendarEntityType.SAVINGS_CHARGES.getValue());
	            }
	        } else {
	            LocalDate calendarStartDate = savingsAccountCharge.getApplicableDueDate();
	            
	            //Mapping ChargeTimeType to PeriodFrequencyType
	            final ChargeTimeType chargeTimeType = savingsAccountCharge.getChargeTimeType();
	            final Integer frequency = savingsAccountCharge.getFeeInterval();
	
	            final Integer repeatsOnDay = calendarStartDate.getDayOfWeek();
	            final String title = "savings_recurring_charge_" + savingsAccountCharge.getId();
	            final Calendar calendar = Calendar.createRepeatingCalendar(title, calendarStartDate, CalendarType.COLLECTION.getValue(),
	                    CalendarFrequencyType.from(chargeTimeType), frequency, repeatsOnDay);
	            calendarInstance = CalendarInstance.from(calendar, savingsAccountCharge.getId(), CalendarEntityType.SAVINGS_CHARGES.getValue());
	        }
	        if (calendarInstance == null) {
	            final String defaultUserMessage = "No valid charge details available for recurring charge schedule creation.";
	            throw new GeneralPlatformDomainRuleException(
	                    "error.msg.savings.account.cannot.create.no.valid.recurring.charge.details.available", defaultUserMessage,
	                    account.clientId());
	        }
        return calendarInstance;
    }
}