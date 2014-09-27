/**
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.mifosplatform.portfolio.savings.domain;

import static org.mifosplatform.portfolio.savings.SavingsApiConstants.amountParamName;
import static org.mifosplatform.portfolio.savings.SavingsApiConstants.calendarInheritedParamName;
import static org.mifosplatform.portfolio.savings.SavingsApiConstants.dateFormatParamName;
import static org.mifosplatform.portfolio.savings.SavingsApiConstants.dueAsOfDateParamName;
import static org.mifosplatform.portfolio.savings.SavingsApiConstants.feeIntervalParamName;
import static org.mifosplatform.portfolio.savings.SavingsApiConstants.feeOnMonthDayParamName;
import static org.mifosplatform.portfolio.savings.SavingsApiConstants.localeParamName;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OrderBy;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.joda.time.LocalDate;
import org.joda.time.MonthDay;
import org.joda.time.Months;
import org.joda.time.Weeks;
import org.joda.time.YearMonth;
import org.joda.time.Years;
import org.mifosplatform.infrastructure.core.api.JsonCommand;
import org.mifosplatform.infrastructure.core.service.DateUtils;
import org.mifosplatform.organisation.holiday.domain.Holiday;
import org.mifosplatform.organisation.holiday.service.HolidayUtil;
import org.mifosplatform.organisation.monetary.domain.MonetaryCurrency;
import org.mifosplatform.organisation.monetary.domain.Money;
import org.mifosplatform.organisation.workingdays.domain.WorkingDays;
import org.mifosplatform.portfolio.calendar.domain.Calendar;
import org.mifosplatform.portfolio.calendar.domain.CalendarFrequencyType;
import org.mifosplatform.portfolio.calendar.service.CalendarUtils;
import org.mifosplatform.portfolio.charge.domain.Charge;
import org.mifosplatform.portfolio.charge.domain.ChargeCalculationType;
import org.mifosplatform.portfolio.charge.domain.ChargeTimeType;
import org.mifosplatform.portfolio.charge.exception.SavingsAccountChargeWithoutMandatoryFieldException;
import org.springframework.data.jpa.domain.AbstractPersistable;
import org.springframework.transaction.annotation.Transactional;

@Entity
@Table(name = "m_savings_account_charge")
public class SavingsAccountCharge extends AbstractPersistable<Long> {
	
    @ManyToOne(optional = false)
    @JoinColumn(name = "savings_account_id", referencedColumnName = "id", nullable = false)
    private SavingsAccount savingsAccount;

    @ManyToOne(optional = false)
    @JoinColumn(name = "charge_id", referencedColumnName = "id", nullable = false)
    private Charge charge;
    
    @OrderBy(value = "installmentNumber, dueDate")
    @OneToMany(mappedBy = "savingsAccountCharge", cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.EAGER)
    private List<SavingsAccountChargeScheduleInstallment> savingsAccountChargeScheduleInstallments;
    
    @Column(name = "charge_time_enum", nullable = false)
    private Integer chargeTime;
    
    @Temporal(TemporalType.DATE)
    @Column(name = "start_date", nullable = false)
    private Date startDate;

    @Temporal(TemporalType.DATE)
    @Column(name = "charge_due_date")
    private Date dueDate;

    @Column(name = "fee_on_month", nullable = true)
    private Integer feeOnMonth;

    @Column(name = "fee_on_day", nullable = true)
    private Integer feeOnDay;

    @Column(name = "fee_interval", nullable = true)
    private Integer feeInterval;

    @Column(name = "charge_calculation_enum")
    private Integer chargeCalculation;

    @Column(name = "calculation_percentage", scale = 6, precision = 19, nullable = true)
    private BigDecimal percentage;

    // TODO AA: This field may not require for savings charges
    @Column(name = "calculation_on_amount", scale = 6, precision = 19, nullable = true)
    private BigDecimal amountPercentageAppliedTo;

    @Column(name = "amount", scale = 6, precision = 19, nullable = false)
    private BigDecimal amount;

    @Column(name = "amount_paid_derived", scale = 6, precision = 19, nullable = true)
    private BigDecimal amountPaid;

    @Column(name = "amount_waived_derived", scale = 6, precision = 19, nullable = true)
    private BigDecimal amountWaived;

    @Column(name = "amount_writtenoff_derived", scale = 6, precision = 19, nullable = true)
    private BigDecimal amountWrittenOff;

    @Column(name = "amount_outstanding_derived", scale = 6, precision = 19, nullable = false)
    private BigDecimal amountOutstanding;

    @Column(name = "is_penalty", nullable = false)
    private boolean penaltyCharge = false;

    @Column(name = "is_paid_derived", nullable = false)
    private boolean paid = false;

    @Column(name = "waived", nullable = false)
    private boolean waived = false;

    @Column(name = "is_active", nullable = false)
    private boolean status = true;
    
    @Temporal(TemporalType.DATE)
    @Column(name = "inactivated_on_date")
    private Date inactivationDate;
    
    @Column(name = "is_calendar_inherited", nullable = false)
    private boolean isCalendarInherited = false;

    public static SavingsAccountCharge createNewFromJson(final SavingsAccount savingsAccount, final Charge chargeDefinition,
            final JsonCommand command) {

        BigDecimal amount = command.bigDecimalValueOfParameterNamed(amountParamName);
        final LocalDate dueDate = command.localDateValueOfParameterNamed(dueAsOfDateParamName);
        MonthDay feeOnMonthDay = command.extractMonthDayNamed(feeOnMonthDayParamName);
        Integer feeInterval = command.integerValueOfParameterNamed(feeIntervalParamName);
        final Boolean isCalendarInherited = command.booleanObjectValueOfParameterNamed(calendarInheritedParamName);
        final ChargeTimeType chargeTime = null;
        final ChargeCalculationType chargeCalculation = null;
        final boolean status = true;

        // If these values is not sent as parameter, then derive from Charge
        // definition
        amount = (amount == null) ? chargeDefinition.getAmount() : amount;
        feeOnMonthDay = (feeOnMonthDay == null) ? chargeDefinition.getFeeOnMonthDay() : feeOnMonthDay;
        feeInterval = (feeInterval == null) ? chargeDefinition.getFeeInterval() : feeInterval;

        return new SavingsAccountCharge(savingsAccount, chargeDefinition, amount, chargeTime, chargeCalculation, dueDate, status,
                feeOnMonthDay, feeInterval, isCalendarInherited);
    }

    public static SavingsAccountCharge createNewWithoutSavingsAccount(final Charge chargeDefinition, final BigDecimal amountPayable,
            final ChargeTimeType chargeTime, final ChargeCalculationType chargeCalculation, final LocalDate dueDate, final boolean status,
            final MonthDay feeOnMonthDay, final Integer feeInterval, final Boolean isCalendarInherited) {
        return new SavingsAccountCharge(null, chargeDefinition, amountPayable, chargeTime, chargeCalculation, dueDate, status,
                feeOnMonthDay, feeInterval, isCalendarInherited);
    }

    protected SavingsAccountCharge() {
        //
    }

    private SavingsAccountCharge(final SavingsAccount savingsAccount, final Charge chargeDefinition, final BigDecimal amount,
            final ChargeTimeType chargeTime, final ChargeCalculationType chargeCalculation, final LocalDate dueDate, final boolean status,
            MonthDay feeOnMonthDay, final Integer feeInterval, final Boolean isCalendarInherited) {

        this.savingsAccount = savingsAccount;
        this.charge = chargeDefinition;
        this.penaltyCharge = chargeDefinition.isPenalty();
        this.chargeTime = (chargeTime == null) ? chargeDefinition.getChargeTime() : chargeTime.getValue();
        

        if (isOnSpecifiedDueDate()) {
            if (dueDate == null) {
                final String defaultUserMessage = "Savings Account charge is missing due date.";
                throw new SavingsAccountChargeWithoutMandatoryFieldException("savingsaccount.charge", dueAsOfDateParamName,
                        defaultUserMessage, chargeDefinition.getId(), chargeDefinition.getName());
            }
            this.startDate = dueDate.toDate();
        }

        if (isAnnualFee() || isMonthlyFee()) {
            feeOnMonthDay = (feeOnMonthDay == null) ? chargeDefinition.getFeeOnMonthDay() : feeOnMonthDay;
            if (feeOnMonthDay == null) {
                final String defaultUserMessage = "Savings Account charge is missing due date.";
                throw new SavingsAccountChargeWithoutMandatoryFieldException("savingsaccount.charge", dueAsOfDateParamName,
                        defaultUserMessage, chargeDefinition.getId(), chargeDefinition.getName());
            }

            this.feeOnMonth = feeOnMonthDay.getMonthOfYear();
            this.feeOnDay = feeOnMonthDay.getDayOfMonth();
            this.startDate = getApplicableDueDate().toDate();

        } else if (isWeeklyFee()) {
            if (dueDate == null) {
                final String defaultUserMessage = "Savings Account charge is missing due date.";
                throw new SavingsAccountChargeWithoutMandatoryFieldException("savingsaccount.charge", dueAsOfDateParamName,
                        defaultUserMessage, chargeDefinition.getId(), chargeDefinition.getName());
            }
            /**
             * For Weekly fee feeOnDay is ISO standard day of the week.
             * Monday=1, Tuesday=2
             */
            this.feeOnDay = dueDate.getDayOfWeek();
            this.startDate = dueDate.toDate();
        } else {
            this.feeOnDay = null;
            this.feeOnMonth = null;
            this.feeInterval = null;
        }

        if (isMonthlyFee() || isWeeklyFee()) {
            this.feeInterval = (feeInterval == null) ? chargeDefinition.feeInterval() : feeInterval;
        }

        this.dueDate = (dueDate == null) ? null : dueDate.toDate();

        this.chargeCalculation = chargeDefinition.getChargeCalculation();
        if (chargeCalculation != null) {
            this.chargeCalculation = chargeCalculation.getValue();
        }

        BigDecimal chargeAmount = chargeDefinition.getAmount();
        if (amount != null) {
            chargeAmount = amount;
        }

        final BigDecimal transactionAmount = new BigDecimal(0);

        populateDerivedFields(transactionAmount, chargeAmount);

        if (this.isWithdrawalFee()) {
            this.amountOutstanding = BigDecimal.ZERO;
        }
        
        this.paid = false;
        this.status = status;
        
        if(isCalendarInherited != null && isRecurringFee())
        	this.isCalendarInherited = isCalendarInherited;
    }

    public void resetPropertiesForRecurringFees() {
        if (isMonthlyFee() || isAnnualFee() || isWeeklyFee()) {
            // FIXME: AA: If charge is percentage of x amount then need to
            // update amount outstanding accordingly.
            // Right now annual and monthly charges supports charge calculation
            // type flat.
            this.amountOutstanding = this.amount;
            this.paid = false;// reset to false for recurring fee.
            this.waived = false;
        }
    }

    private void populateDerivedFields(final BigDecimal transactionAmount, final BigDecimal chargeAmount) {

        switch (ChargeCalculationType.fromInt(this.chargeCalculation)) {
            case INVALID:
                this.percentage = null;
                this.amount = null;
                this.amountPercentageAppliedTo = null;
                this.amountPaid = null;
                this.amountOutstanding = BigDecimal.ZERO;
                this.amountWaived = null;
                this.amountWrittenOff = null;
            break;
            case FLAT:
                this.percentage = null;
                this.amount = chargeAmount;
                this.amountPercentageAppliedTo = null;
                this.amountPaid = null;
                this.amountOutstanding = chargeAmount;
                this.amountWaived = null;
                this.amountWrittenOff = null;
            break;
            case PERCENT_OF_AMOUNT:
                this.percentage = chargeAmount;
                this.amountPercentageAppliedTo = transactionAmount;
                this.amount = percentageOf(this.amountPercentageAppliedTo, this.percentage);
                this.amountPaid = null;
                this.amountOutstanding = calculateOutstandingLocal();
                this.amountWaived = null;
                this.amountWrittenOff = null;
            break;
            case PERCENT_OF_AMOUNT_AND_INTEREST:
                this.percentage = null;
                this.amount = null;
                this.amountPercentageAppliedTo = null;
                this.amountPaid = null;
                this.amountOutstanding = BigDecimal.ZERO;
                this.amountWaived = null;
                this.amountWrittenOff = null;
            break;
            case PERCENT_OF_INTEREST:
                this.percentage = null;
                this.amount = null;
                this.amountPercentageAppliedTo = null;
                this.amountPaid = null;
                this.amountOutstanding = BigDecimal.ZERO;
                this.amountWaived = null;
                this.amountWrittenOff = null;
            break;
        }
    }

    public void markAsFullyPaid() {
        this.amountPaid = this.amount;
        this.amountOutstanding = BigDecimal.ZERO;
        this.paid = true;
    }

    public void resetToOriginal(final MonetaryCurrency currency) {
        this.amountPaid = BigDecimal.ZERO;
        this.amountWaived = BigDecimal.ZERO;
        this.amountWrittenOff = BigDecimal.ZERO;
        this.amountOutstanding = calculateAmountOutstanding(currency);
        this.paid = false;
        this.waived = false;
    }
    
    private Money handleInstallmentPayment(final SavingsAccountChargeScheduleInstallment currentInstallment,
            final Money transactionAmountUnprocessed, final LocalDate transactionDate) {

        Money transactionAmountRemaining = transactionAmountUnprocessed;
        Money depositAmountPortion = Money.zero(transactionAmountRemaining.getCurrency());

        depositAmountPortion = currentInstallment.payInstallment(transactionDate, transactionAmountRemaining);
        transactionAmountRemaining = transactionAmountRemaining.minus(depositAmountPortion);

        return transactionAmountRemaining;

    }
    
    public Money pay(final MonetaryCurrency currency, Money amountPaid,
    		final LocalDate transactionDate) {
        Money amountPaidToDate = Money.of(currency, this.amountPaid); 
        Money amountOutstanding = Money.of(currency, this.amountOutstanding);
        Money transactionAmountRemaining = amountPaid;
        
        if(isRecurringFee()) {
        	
        	List<SavingsAccountChargeScheduleInstallment> installments = 
        			getSavingsAccountChargeScheduleInstallments();
        			
        	for(SavingsAccountChargeScheduleInstallment installment : installments) {
        		if (installment.isNotFullyPaidOff() && transactionAmountRemaining.isGreaterThanZero()) {
                    if (installment.getDueDate().isAfter(getDueLocalDate())) {
                    	break;
                    }
                    transactionAmountRemaining = handleInstallmentPayment(installment,
                    		transactionAmountRemaining, transactionDate);
                }
        	}
        	
        }
        
        if(transactionAmountRemaining.isEqualTo(Money.zero(currency))) {
	    	amountPaidToDate = amountPaidToDate.plus(amountPaid);
	    	amountOutstanding = amountOutstanding.minus(amountPaid);
        } else {
        	Money actualAmountPaid = amountPaid.minus(transactionAmountRemaining);
        	amountPaidToDate = amountPaidToDate.plus(actualAmountPaid);
	    	amountOutstanding = amountOutstanding.minus(actualAmountPaid);
        }
        	
        this.amountPaid = amountPaidToDate.getAmount();
        this.amountOutstanding = amountOutstanding.getAmount();
        this.paid = determineIfFullyPaid();

        if (BigDecimal.ZERO.compareTo(this.amountOutstanding) == 0) {
            // full outstanding is paid, update to next due date
        	updateToNextDueDate();
            resetPropertiesForRecurringFees();
        }

        return amountPaid;
    }
    
    private Money handleInstallmentWaive(final SavingsAccountChargeScheduleInstallment currentInstallment,
            final Money transactionAmountUnprocessed, final LocalDate transactionDate) {

        Money transactionAmountRemaining = transactionAmountUnprocessed;
        Money waiveAmountPortion = Money.zero(transactionAmountRemaining.getCurrency());

        waiveAmountPortion = currentInstallment.waive(transactionDate, transactionAmountRemaining);
        transactionAmountRemaining = transactionAmountRemaining.minus(waiveAmountPortion);

        return transactionAmountRemaining;

    }
    
    public Money waive(final MonetaryCurrency currency, Money amountWaived,
    		final LocalDate transactionDate) {
        Money amountWaivedToDate = Money.of(currency, this.amountWaived);
        Money amountOutstanding = Money.of(currency, this.amountOutstanding);
        Money transactionAmountRemaining = amountWaived;
        
        if(isRecurringFee()) {
        	List<SavingsAccountChargeScheduleInstallment> installments = 
        			getSavingsAccountChargeScheduleInstallments();
        	
        	for(SavingsAccountChargeScheduleInstallment installment : installments) {
        		if (installment.isNotFullyPaidOff() && amountWaived.isGreaterThanZero()) {
                    if (installment.getDueDate().isAfter(getDueLocalDate())) {
                    	break;
                    }
                    transactionAmountRemaining = handleInstallmentWaive(installment,
                    		transactionAmountRemaining, transactionDate);
        		}
        	}
        }
        
        if(transactionAmountRemaining.isEqualTo(Money.zero(currency))) {
        	amountWaivedToDate = amountWaivedToDate.plus(amountWaived);
            amountOutstanding = amountOutstanding.minus(amountWaived);
        } else {
        	Money actualAmountWaived = amountWaived.minus(transactionAmountRemaining);
        	amountWaivedToDate = amountWaivedToDate.plus(actualAmountWaived);
	    	amountOutstanding = amountOutstanding.minus(actualAmountWaived);
        }
        
        
        this.amountWaived = amountWaivedToDate.getAmount();
        this.amountOutstanding = amountOutstanding.getAmount();
        
        this.waived = determineIfFullyPaid();

        if (BigDecimal.ZERO.compareTo(this.amountOutstanding) == 0) {
            // full outstanding is waived, update to next due date
        	updateToNextDueDate();
            resetPropertiesForRecurringFees();
        }

        return amountWaived;
    }
    
    private Money handleUndoWaiver(final SavingsAccountChargeScheduleInstallment currentInstallment,
            final Money transactionAmountUnprocessed) {

        Money transactionAmountRemaining = transactionAmountUnprocessed;
        Money undoneAmountPortion = Money.zero(transactionAmountRemaining.getCurrency());

        undoneAmountPortion = currentInstallment.undoWaive(transactionAmountRemaining);
        transactionAmountRemaining = transactionAmountRemaining.minus(undoneAmountPortion);

        return transactionAmountRemaining;
    }
    
    private Money handleUndoPayment(final SavingsAccountChargeScheduleInstallment currentInstallment,
            final Money transactionAmountUnprocessed) {

        Money transactionAmountRemaining = transactionAmountUnprocessed;
        Money undoneAmountPortion = Money.zero(transactionAmountRemaining.getCurrency());

        undoneAmountPortion = currentInstallment.undoPayment(transactionAmountRemaining);
        transactionAmountRemaining = transactionAmountRemaining.minus(undoneAmountPortion);

        return transactionAmountRemaining;

    }

    public void undoWaiver(final MonetaryCurrency currency, final Money transactionAmount) {
        Money amountWaived = getAmountWaived(currency);
        amountWaived = amountWaived.minus(transactionAmount);
        this.amountWaived = amountWaived.getAmount();
        
        Money transactionAmountRemaining = transactionAmount;
        
        if(isRecurringFee()) {
        	
        	ListIterator<SavingsAccountChargeScheduleInstallment> iter = 
        			this.savingsAccountChargeScheduleInstallments.listIterator
        			(this.savingsAccountChargeScheduleInstallments.size());
        	
        	while (iter.hasPrevious()) {
        		SavingsAccountChargeScheduleInstallment installment = iter.previous();
            	if (installment.getWaivedAmount(currency).isNotEqualTo(Money.zero(currency))
            			 && transactionAmountRemaining.isGreaterThanZero()) {
            		transactionAmountRemaining = handleUndoWaiver(installment, transactionAmountRemaining);
                }
            }
        	
        	this.amountOutstanding = calculateOutstandingSchedule();
        } else
        	this.amountOutstanding = calculateOutstandingLocal();
        
        this.paid = false;
        this.waived = false;
        this.status = true;
    }

    public void undoPayment(final MonetaryCurrency currency, final Money transactionAmount) {
        Money amountPaid = getAmountPaid(currency);
        amountPaid = amountPaid.minus(transactionAmount);
        this.amountPaid = amountPaid.getAmount();
        
        Money transactionAmountRemaining = transactionAmount;
        
        if(isRecurringFee()) {
        	
        	ListIterator<SavingsAccountChargeScheduleInstallment> iter = 
        			this.savingsAccountChargeScheduleInstallments.listIterator
        			(this.savingsAccountChargeScheduleInstallments.size());
        	
        	while (iter.hasPrevious()) {
        		SavingsAccountChargeScheduleInstallment installment = iter.previous();
            	if (installment.getPaidAmount(currency).isNotEqualTo(Money.zero(currency))
            			&& transactionAmountRemaining.isGreaterThanZero()) {
            		transactionAmountRemaining = handleUndoPayment(installment, transactionAmountRemaining);
                }
            }
        	
        	this.amountOutstanding = calculateOutstandingSchedule();
        } else
        	this.amountOutstanding = calculateOutstandingLocal();
        
        if (this.isWithdrawalFee()) {
            this.amountOutstanding = BigDecimal.ZERO;
        }
        
        this.paid = false;
        this.waived = false;
        this.status = true;
    }
    
    private BigDecimal calculateAmountOutstanding(final MonetaryCurrency currency) {
        return getAmount(currency).minus(getAmountWaived(currency)).minus(getAmountPaid(currency)).getAmount();
    }

    public void update(final SavingsAccount savingsAccount) {
        this.savingsAccount = savingsAccount;
    }

    public void update(final BigDecimal amount, final LocalDate dueDate, final MonthDay feeOnMonthDay, final Integer feeInterval,
    		final Boolean isCalendarInherited) {
        final BigDecimal transactionAmount = BigDecimal.ZERO;
        if (dueDate != null) {
            this.dueDate = dueDate.toDate();
            if (isWeeklyFee()) {
                this.feeOnDay = dueDate.getDayOfWeek();
            }
        }

        if (feeOnMonthDay != null) {
            this.feeOnMonth = feeOnMonthDay.getMonthOfYear();
            this.feeOnDay = feeOnMonthDay.getDayOfMonth();
        }

        if (feeInterval != null) {
            this.feeInterval = feeInterval;
        }
        
        if (isCalendarInherited != null) {
            this.isCalendarInherited = isCalendarInherited;
        }

        if (amount != null) {
            switch (ChargeCalculationType.fromInt(this.chargeCalculation)) {
                case INVALID:
                break;
                case FLAT:
                    this.amount = amount;
                break;
                case PERCENT_OF_AMOUNT:
                    this.percentage = amount;
                    this.amountPercentageAppliedTo = transactionAmount;
                    this.amount = percentageOf(this.amountPercentageAppliedTo, this.percentage);
                    this.amountOutstanding = calculateOutstandingLocal();
                break;
                case PERCENT_OF_AMOUNT_AND_INTEREST:
                    this.percentage = amount;
                    this.amount = null;
                    this.amountPercentageAppliedTo = null;
                    this.amountOutstanding = null;
                break;
                case PERCENT_OF_INTEREST:
                    this.percentage = amount;
                    this.amount = null;
                    this.amountPercentageAppliedTo = null;
                    this.amountOutstanding = null;
                break;
            }
        }
    }

    public Map<String, Object> update(final JsonCommand command) {

        final Map<String, Object> actualChanges = new LinkedHashMap<>(7);

        final String dateFormatAsInput = command.dateFormat();
        final String localeAsInput = command.locale();

        if (command.isChangeInLocalDateParameterNamed(dueAsOfDateParamName, getDueLocalDate())) {
            final String valueAsInput = command.stringValueOfParameterNamed(dueAsOfDateParamName);
            actualChanges.put(dueAsOfDateParamName, valueAsInput);
            actualChanges.put(dateFormatParamName, dateFormatAsInput);
            actualChanges.put(localeParamName, localeAsInput);

            final LocalDate newValue = command.localDateValueOfParameterNamed(dueAsOfDateParamName);
            this.dueDate = newValue.toDate();
            if (this.isWeeklyFee()) {
                this.feeOnDay = newValue.getDayOfWeek();
            }
        }

        if (command.hasParameter(feeOnMonthDayParamName)) {
            final MonthDay monthDay = command.extractMonthDayNamed(feeOnMonthDayParamName);
            final String actualValueEntered = command.stringValueOfParameterNamed(feeOnMonthDayParamName);
            final Integer dayOfMonthValue = monthDay.getDayOfMonth();
            if (this.feeOnDay != dayOfMonthValue) {
                actualChanges.put(feeOnMonthDayParamName, actualValueEntered);
                actualChanges.put(localeParamName, localeAsInput);
                this.feeOnDay = dayOfMonthValue;
            }

            final Integer monthOfYear = monthDay.getMonthOfYear();
            if (this.feeOnMonth != monthOfYear) {
                actualChanges.put(feeOnMonthDayParamName, actualValueEntered);
                actualChanges.put(localeParamName, localeAsInput);
                this.feeOnMonth = monthOfYear;
            }
        }

        if (command.isChangeInBigDecimalParameterNamed(amountParamName, this.amount)) {
            final BigDecimal newValue = command.bigDecimalValueOfParameterNamed(amountParamName);
            actualChanges.put(amountParamName, newValue);
            actualChanges.put(localeParamName, localeAsInput);
            switch (ChargeCalculationType.fromInt(this.chargeCalculation)) {
                case INVALID:
                break;
                case FLAT:
                    this.amount = newValue;
                    this.amountOutstanding = calculateOutstandingLocal();
                break;
                case PERCENT_OF_AMOUNT:
                    this.percentage = newValue;
                    this.amountPercentageAppliedTo = null;
                    this.amount = percentageOf(this.amountPercentageAppliedTo, this.percentage);
                    this.amountOutstanding = calculateOutstandingLocal();
                break;
                case PERCENT_OF_AMOUNT_AND_INTEREST:
                    this.percentage = newValue;
                    this.amount = null;
                    this.amountPercentageAppliedTo = null;
                    this.amountOutstanding = null;
                break;
                case PERCENT_OF_INTEREST:
                    this.percentage = newValue;
                    this.amount = null;
                    this.amountPercentageAppliedTo = null;
                    this.amountOutstanding = null;
                break;
            }
            
            final List<SavingsAccountChargeScheduleInstallment> savingsAccountChargeScheduleInstallments = 
            		getSavingsAccountChargeScheduleInstallments();
            for(SavingsAccountChargeScheduleInstallment installment : savingsAccountChargeScheduleInstallments)
            	installment.updateAmount(newValue);
        }

        return actualChanges;
    }

    private boolean isGreaterThanZero(final BigDecimal value) {
        return value.compareTo(BigDecimal.ZERO) == 1;
    }

    public LocalDate getDueLocalDate() {
        LocalDate dueDate = null;
        if (this.dueDate != null) {
            dueDate = new LocalDate(this.dueDate);
        }
        
        return dueDate;
    }
    
    public LocalDate getApplicableDueDate() {
    	LocalDate dueDate = getDueLocalDate();
    	LocalDate today = DateUtils.getLocalDateOfTenant();
        YearMonth currentYearMonth = new YearMonth(today);
        
       //If due date has already passed in current year, get next due date.
        if (isAnnualFee()) {
        	dueDate = new LocalDate().withMonthOfYear(this.feeOnMonth);
        	dueDate = dueDate.withMonthOfYear(this.feeOnMonth);
        	if(dueDate.isBefore(today))
        		dueDate = calculateNextDueDate(today);
        } else if (isMonthlyFee()) {
        	
        	//If due date is of a previous month, get a due date with month >= this month.
        	dueDate = new LocalDate().withMonthOfYear(this.feeOnMonth);
        	dueDate = dueDate.withDayOfMonth(this.feeOnDay);
        	while(currentYearMonth.getMonthOfYear() > dueDate.getMonthOfYear())
    			dueDate = calculateNextDueDate(today);
        }
        return dueDate;
    }

    private boolean determineIfFullyPaid() {
    	if(isRecurringFee())
    		return BigDecimal.ZERO.compareTo(calculateOutstandingSchedule()) == 0;
    	return BigDecimal.ZERO.compareTo(calculateOutstandingLocal()) == 0;
    }

    private BigDecimal calculateOutstandingLocal() {

        BigDecimal amountPaidLocal = BigDecimal.ZERO;
        if (this.amountPaid != null) {
            amountPaidLocal = this.amountPaid;
        }

        BigDecimal amountWaivedLocal = BigDecimal.ZERO;
        if (this.amountWaived != null) {
            amountWaivedLocal = this.amountWaived;
        }

        BigDecimal amountWrittenOffLocal = BigDecimal.ZERO;
        if (this.amountWrittenOff != null) {
            amountWrittenOffLocal = this.amountWrittenOff;
        }

        final BigDecimal totalAccountedFor = amountPaidLocal.add(amountWaivedLocal).add(amountWrittenOffLocal);

        return this.amount.subtract(totalAccountedFor);
    }
    
    private BigDecimal calculateOutstandingSchedule() {
    	
    	BigDecimal totalOutstandingSchedule = BigDecimal.ZERO;
    	ListIterator<SavingsAccountChargeScheduleInstallment> iter = 
    			getSavingsAccountChargeScheduleInstallments().listIterator();
    	SavingsAccountChargeScheduleInstallment installment;
    	
    	do{
    		installment = iter.next();
        	totalOutstandingSchedule = totalOutstandingSchedule.add(installment
        			.getInstallmentAmountOverdue(this.savingsAccount.getCurrency()).getAmount());
    	}while(!installment.getDueDate().isEqual(getDueLocalDate()) && iter.hasNext());

        return totalOutstandingSchedule;
    }

    private BigDecimal percentageOf(final BigDecimal value, final BigDecimal percentage) {

        BigDecimal percentageOf = BigDecimal.ZERO;

        if (isGreaterThanZero(value)) {
            final MathContext mc = new MathContext(8, RoundingMode.HALF_EVEN);
            final BigDecimal multiplicand = percentage.divide(BigDecimal.valueOf(100l), mc);
            percentageOf = value.multiply(multiplicand, mc);
        }

        return percentageOf;
    }

    public BigDecimal amount() {
        return this.amount;
    }

    public BigDecimal amoutOutstanding() {
        return this.amountOutstanding;
    }

    public boolean isFeeCharge() {
        return !this.penaltyCharge;
    }

    public boolean isPenaltyCharge() {
        return this.penaltyCharge;
    }

    public boolean isNotFullyPaid() {
        return !isPaid();
    }

    public boolean isPaid() {
        return this.paid;
    }

    public boolean isWaived() {
        return this.waived;
    }

    public boolean isPaidOrPartiallyPaid(final MonetaryCurrency currency) {

        final Money amountWaivedOrWrittenOff = getAmountWaived(currency).plus(getAmountWrittenOff(currency));
        return Money.of(currency, this.amountPaid).plus(amountWaivedOrWrittenOff).isGreaterThanZero();
    }

    public Money getAmount(final MonetaryCurrency currency) {
        return Money.of(currency, this.amount);
    }

    private Money getAmountPaid(final MonetaryCurrency currency) {
        return Money.of(currency, this.amountPaid);
    }

    public Money getAmountWaived(final MonetaryCurrency currency) {
        return Money.of(currency, this.amountWaived);
    }

    public Money getAmountWrittenOff(final MonetaryCurrency currency) {
        return Money.of(currency, this.amountWrittenOff);
    }

    public Money getAmountOutstanding(final MonetaryCurrency currency) {
        return Money.of(currency, this.amountOutstanding);
    }
    
    public Integer getFeeInterval() {
    	if(isAnnualFee())
    		return 1;
		return this.feeInterval;
    }

    /**
     * @param incrementBy
     *            Amount used to pay off this charge
     * @return Actual amount paid on this charge
     */
    public Money updatePaidAmountBy(final Money incrementBy) {

        Money amountPaidToDate = Money.of(incrementBy.getCurrency(), this.amountPaid);
        final Money amountOutstanding = Money.of(incrementBy.getCurrency(), this.amountOutstanding);

        Money amountPaidOnThisCharge = Money.zero(incrementBy.getCurrency());
        if (incrementBy.isGreaterThanOrEqualTo(amountOutstanding)) {
            amountPaidOnThisCharge = amountOutstanding;
            amountPaidToDate = amountPaidToDate.plus(amountOutstanding);
            this.amountPaid = amountPaidToDate.getAmount();
            this.amountOutstanding = BigDecimal.ZERO;
        } else {
            amountPaidOnThisCharge = incrementBy;
            amountPaidToDate = amountPaidToDate.plus(incrementBy);
            this.amountPaid = amountPaidToDate.getAmount();

            final Money amountExpected = Money.of(incrementBy.getCurrency(), this.amount);
            this.amountOutstanding = amountExpected.minus(amountPaidToDate).getAmount();
        }

        this.paid = determineIfFullyPaid();

        return amountPaidOnThisCharge;
    }

    public String name() {
        return this.charge.getName();
    }

    public String currencyCode() {
        return this.charge.getCurrencyCode();
    }

    public Charge getCharge() {
        return this.charge;
    }

    public SavingsAccount savingsAccount() {
        return this.savingsAccount;
    }

    public boolean isOnSpecifiedDueDate() {
        return ChargeTimeType.fromInt(this.chargeTime).isOnSpecifiedDueDate();
    }

    public boolean isSavingsActivation() {
        return ChargeTimeType.fromInt(this.chargeTime).isSavingsActivation();
    }

    public boolean isSavingsClosure() {
        return ChargeTimeType.fromInt(this.chargeTime).isSavingsClosure();
    }

    public boolean isWithdrawalFee() {
        return ChargeTimeType.fromInt(this.chargeTime).isWithdrawalFee();
    }

    public boolean isOverdraftFee() {
        return ChargeTimeType.fromInt(this.chargeTime).isOverdraftFee();
    }

    public boolean isAnnualFee() {
        return ChargeTimeType.fromInt(this.chargeTime).isAnnualFee();
    }

    public boolean isMonthlyFee() {
        return ChargeTimeType.fromInt(this.chargeTime).isMonthlyFee();
    }

    public boolean isWeeklyFee() {
        return ChargeTimeType.fromInt(this.chargeTime).isWeeklyFee();
    }
    
    public ChargeTimeType getChargeTimeType() {
    	return ChargeTimeType.fromInt(this.chargeTime);
    }

    public boolean hasCurrencyCodeOf(final String matchingCurrencyCode) {
        if (this.currencyCode() == null || matchingCurrencyCode == null) { return false; }
        return this.currencyCode().equalsIgnoreCase(matchingCurrencyCode);
    }
    
    public boolean isCalendarInherited() {
        return this.isCalendarInherited;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) { return false; }
        if (obj == this) { return true; }
        if (obj.getClass() != getClass()) { return false; }
        final SavingsAccountCharge rhs = (SavingsAccountCharge) obj;
        return new EqualsBuilder().appendSuper(super.equals(obj)) //
                .append(getId(), rhs.getId()) //
                .append(this.chargeTime, rhs.chargeTime)
                .append(this.amount, rhs.amount) //
                .append(this.amountPaid, rhs.amountPaid) //
                .append(this.amountOutstanding, rhs.amountOutstanding) //
                .append(getDueLocalDate(), rhs.getDueLocalDate()) //
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(3, 5) //
        .append(getId()) //
        .append(this.amount) //
        .append(this.amountPaid) //
        .append(this.amountOutstanding) //
        .append(this.chargeTime) //
        .append(getDueLocalDate()) //
        .toHashCode();
    }

    public BigDecimal updateWithdralFeeAmount(final BigDecimal transactionAmount) {
        BigDecimal amountPaybale = BigDecimal.ZERO;
        if (ChargeCalculationType.fromInt(this.chargeCalculation).isFlat()) {
            amountPaybale = this.amount;
        } else if (ChargeCalculationType.fromInt(this.chargeCalculation).isPercentageOfAmount()) {
            amountPaybale = transactionAmount.multiply(this.percentage).divide(BigDecimal.valueOf(100l));
        }
        this.amountOutstanding = amountPaybale;
        return amountPaybale;
    }

    public void updateToNextDueDateFrom(final LocalDate startingDate) {
        if (isAnnualFee() || isMonthlyFee() || isWeeklyFee()) {
            LocalDate nextDueLocalDate = null;
            if (isAnnualFee() || isMonthlyFee()) {
                nextDueLocalDate = startingDate.withMonthOfYear(this.feeOnMonth);
                nextDueLocalDate = setDayOfMonth(nextDueLocalDate);
                while (startingDate.isAfter(nextDueLocalDate)) {
                    nextDueLocalDate = calculateNextDueDate(nextDueLocalDate);
                }
            } else if (isWeeklyFee()) {
                nextDueLocalDate = getDueLocalDate();
                while (startingDate.isAfter(nextDueLocalDate)) {
                    nextDueLocalDate = calculateNextDueDate(nextDueLocalDate);
                }
            } else {
                nextDueLocalDate = calculateNextDueDate(startingDate);
            }

            this.dueDate = nextDueLocalDate.toDate();
        }

    }
    
	// From installments
	@Transactional
	public LocalDate updateToNextDueDate() {

		List<SavingsAccountChargeScheduleInstallment> savingsAccountChargeScheduleInstallments = getSavingsAccountChargeScheduleInstallments();
		ListIterator<SavingsAccountChargeScheduleInstallment> iter = savingsAccountChargeScheduleInstallments
				.listIterator(savingsAccountChargeScheduleInstallments.size());
		while (iter.hasPrevious()) {
			SavingsAccountChargeScheduleInstallment installment = iter
					.previous();
			if (installment.getDueDate().isEqual(getDueLocalDate())) {
				iter.next();
				if (iter.hasNext()) {
					installment = iter.next();
					this.dueDate = installment.getDueDate().toDate();
					this.amountOutstanding = this.amountOutstanding.add(amount);
				}
				break;
			}
		}

		return getDueLocalDate();
	}

    private LocalDate calculateNextDueDate(final LocalDate date) {
        LocalDate nextDueLocalDate = null;
        if (isAnnualFee()) {
            nextDueLocalDate = date.withMonthOfYear(this.feeOnMonth).plusYears(1);
            nextDueLocalDate = setDayOfMonth(nextDueLocalDate);
        } else if (isMonthlyFee()) {
            nextDueLocalDate = date.plusMonths(this.feeInterval);
            nextDueLocalDate = setDayOfMonth(nextDueLocalDate);
        } else if (isWeeklyFee()) {
            nextDueLocalDate = date.plusWeeks(this.feeInterval);
            nextDueLocalDate = setDayOfWeek(nextDueLocalDate);
        }
        return nextDueLocalDate;
    }

    private LocalDate setDayOfMonth(LocalDate nextDueLocalDate) {
        int maxDayOfMonth = nextDueLocalDate.dayOfMonth().withMaximumValue().getDayOfMonth();
        int newDayOfMonth = (this.feeOnDay.intValue() < maxDayOfMonth) ? this.feeOnDay.intValue() : maxDayOfMonth;
        nextDueLocalDate = nextDueLocalDate.withDayOfMonth(newDayOfMonth);
        return nextDueLocalDate;
    }

    private LocalDate setDayOfWeek(LocalDate nextDueLocalDate) {
        if (this.feeOnDay != nextDueLocalDate.getDayOfWeek()) {
            nextDueLocalDate = nextDueLocalDate.withDayOfWeek(this.feeOnDay);
        }
        return nextDueLocalDate;
    }

    public void updateNextDueDateForRecurringFees() {
        if (isAnnualFee() || isMonthlyFee() || isWeeklyFee()) {
            LocalDate nextDueLocalDate = new LocalDate(dueDate);
            nextDueLocalDate = calculateNextDueDate(nextDueLocalDate);
            this.dueDate = nextDueLocalDate.toDate();
        }
    }

    public void updateToPreviousDueDate() {
        if (isAnnualFee() || isMonthlyFee() || isWeeklyFee()) {
            LocalDate nextDueLocalDate = new LocalDate(dueDate);
            if (isAnnualFee()) {
                nextDueLocalDate = nextDueLocalDate.withMonthOfYear(this.feeOnMonth).minusYears(1);
                nextDueLocalDate = setDayOfMonth(nextDueLocalDate);
            } else if (isMonthlyFee()) {
                nextDueLocalDate = nextDueLocalDate.minusMonths(this.feeInterval);
                nextDueLocalDate = setDayOfMonth(nextDueLocalDate);
            } else if (isWeeklyFee()) {
                nextDueLocalDate = nextDueLocalDate.minusDays(7 * this.feeInterval);
                nextDueLocalDate = setDayOfWeek(nextDueLocalDate);
            }

            this.dueDate = nextDueLocalDate.toDate();
        }
    }

    public boolean feeSettingsNotSet() {
        return !feeSettingsSet();
    }

    public boolean feeSettingsSet() {
        return this.feeOnDay != null && this.feeOnMonth != null;
    }

    public boolean isRecurringFee() {
        return isWeeklyFee() || isMonthlyFee() || isAnnualFee();
    }

    public boolean isChargeIsDue(final LocalDate nextDueDate) {
        return this.getDueLocalDate().isBefore(nextDueDate);
    }

    public boolean isChargeIsOverPaid(final LocalDate nextDueDate) {
        final BigDecimal amountPaid = this.amountPaid == null ? BigDecimal.ZERO : amountPaid();
        return this.getDueLocalDate().isAfter(nextDueDate) && amountPaid.compareTo(BigDecimal.ZERO) == 1;
    }

    private BigDecimal amountPaid() {
        return this.amountPaid;
    }

    public LocalDate nextDuDate(final LocalDate date) {
        return calculateNextDueDate(date);
    }

    public void inactiavateCharge(final LocalDate inactivationOnDate) {
        this.inactivationDate = inactivationOnDate.toDate();
        this.status = false;
        this.amountOutstanding = BigDecimal.ZERO;
        this.paid = true;
    }

    public boolean isActive() {
        return this.status;
    }

    public boolean isNotActive() {
        return !isActive();
    }
    
    public void generateSchedule(final Calendar calendar, final boolean isHolidayEnabled,
    		final List<Holiday> holidays, final WorkingDays workingDays) {
    	
    	final CalendarFrequencyType frequency = CalendarUtils.getFrequency(calendar.getRecurrence());
        Integer recurringEvery = CalendarUtils.getInterval(calendar.getRecurrence());
        recurringEvery = recurringEvery == -1 ? 1 : recurringEvery;
        
        final List<SavingsAccountChargeScheduleInstallment> savingsAccountChargeScheduleInstallments = 
        		getSavingsAccountChargeScheduleInstallments();
        savingsAccountChargeScheduleInstallments.clear();
        LocalDate installmentDate = null;
        if (isCalendarInherited()) {
            installmentDate = CalendarUtils.getNextScheduleDate(calendar, getApplicableDueDate(), workingDays);
        } else {
            installmentDate = getApplicableDueDate();
        }

        final LocalDate installmentsTillDate = calculateInstallmentsTillDate(installmentDate, frequency, recurringEvery);
        int installmentNumber = 1;
        final BigDecimal dueAmount = this.amount;
        boolean chargeDueDateUpdatedFlag = false;
        
        while (installmentsTillDate.isAfter(installmentDate)) {
        	SavingsAccountChargeScheduleInstallment installment = null;
        	if (isHolidayEnabled) {
        		LocalDate holidayModifiedInstallmentDate = HolidayUtil.getRepaymentRescheduleDateToIfHoliday(installmentDate, holidays);
        		installment = SavingsAccountChargeScheduleInstallment.installment(this,
                        installmentNumber, holidayModifiedInstallmentDate.toDate(), dueAmount);
            } else {
            installment = SavingsAccountChargeScheduleInstallment.installment(this,
                    installmentNumber, installmentDate.toDate(), dueAmount);
            }
        	savingsAccountChargeScheduleInstallments.add(installment);
            
        	if(!chargeDueDateUpdatedFlag) 
        		chargeDueDateUpdatedFlag = updateFields(installment.getDueDate(),
        				this.amount, chargeDueDateUpdatedFlag);
        	
            installmentDate = CalendarUtils.getNextScheduleDate(calendar, installmentDate, workingDays);
            installmentNumber += 1;
        }
    }
    
    //On schedule generation and update
    private boolean updateFields(final LocalDate dueDate, final BigDecimal dueAmount,
    		boolean chargeDueDateUpdatedFlag) {
    	
    	LocalDate today = DateUtils.getLocalDateOfTenant();
        
    	if(!dueDate.isBefore(today)) {
    		this.dueDate = dueDate.toDate();
    		chargeDueDateUpdatedFlag = true;
    	} else
    		this.amountOutstanding = this.amountOutstanding.add(dueAmount);
    	
    	return chargeDueDateUpdatedFlag;
    }
    
    public void updateSchedule(final Calendar calendar, final boolean isHolidayEnabled,
    		final List<Holiday> holidays, final WorkingDays workingDays) {
    	
    	//Reset for recalculation
    	this.amountOutstanding = this.amount;
    	
    	final List<SavingsAccountChargeScheduleInstallment> savingsAccountChargeScheduleInstallments = 
    			getSavingsAccountChargeScheduleInstallments();
    	
    	LocalDate lastInstallmentDate = savingsAccountChargeScheduleInstallments.get(0).getDueDate();
    	boolean chargeDueDateUpdatedFlag = false;
    	
    	for(SavingsAccountChargeScheduleInstallment installment : savingsAccountChargeScheduleInstallments) {
    		LocalDate dueDate = installment.getDueDate();
    		LocalDate meetingChangeDate = calendar.getStartDateLocalDate();
    		if(dueDate.isAfter(meetingChangeDate) || dueDate.isEqual(meetingChangeDate)) {
    			LocalDate nextScheduleDate = CalendarUtils.getNextScheduleDate(calendar, lastInstallmentDate, workingDays);
    			if (isHolidayEnabled) {
            		LocalDate holidayModifiedInstallmentDate = HolidayUtil.getRepaymentRescheduleDateToIfHoliday(nextScheduleDate, holidays);
            		installment.updateDueDate(holidayModifiedInstallmentDate);
                } else {
                	installment.updateDueDate(nextScheduleDate);
                }
    		} 
    		
    		if(!chargeDueDateUpdatedFlag) 
        		chargeDueDateUpdatedFlag = updateFields(installment.getDueDate(),
        				this.amount, chargeDueDateUpdatedFlag);
    		
			lastInstallmentDate = dueDate;
    	}
    	
    }
    
    public List<SavingsAccountChargeScheduleInstallment> getSavingsAccountChargeScheduleInstallments() {
        if (this.savingsAccountChargeScheduleInstallments == null) {
            this.savingsAccountChargeScheduleInstallments = new ArrayList<>();
        }
        return this.savingsAccountChargeScheduleInstallments;
    }
    
    private LocalDate calculateInstallmentsTillDate(final LocalDate startDate, final CalendarFrequencyType frequency,
    		Integer recurringEvery) {

        LocalDate tillDate = null;
        final LocalDate today = DateUtils.getLocalDateOfTenant();
        Integer minNumberOfInstallments = 10;
        boolean additionalInstallments = false;
        if(startDate.isBefore(today))
        	additionalInstallments = true;
        switch (frequency) {
            case WEEKLY:
            	tillDate = (additionalInstallments) ? startDate.plusWeeks(minNumberOfInstallments 
            			* recurringEvery + Weeks.weeksBetween(startDate, today).getWeeks()) :
            				startDate.plusWeeks(minNumberOfInstallments * recurringEvery);
            break;
            case MONTHLY:
            	tillDate = (additionalInstallments) ? startDate.plusMonths(minNumberOfInstallments
            			* recurringEvery + Months.monthsBetween(startDate, today).getMonths()) :
            				startDate.plusMonths(minNumberOfInstallments * recurringEvery);
            break;
            case YEARLY:
            	tillDate = (additionalInstallments) ? startDate.plusYears(minNumberOfInstallments
            			* recurringEvery + Years.yearsBetween(startDate, today).getYears() ) :
            				startDate.plusYears(minNumberOfInstallments * recurringEvery);
            break;
			default:
			break;
        }

        return tillDate;
    }
    
}