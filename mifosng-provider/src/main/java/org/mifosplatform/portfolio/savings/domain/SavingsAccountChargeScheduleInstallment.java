package org.mifosplatform.portfolio.savings.domain;

import java.math.BigDecimal;
import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.joda.time.LocalDate;
import org.mifosplatform.organisation.monetary.domain.MonetaryCurrency;
import org.mifosplatform.organisation.monetary.domain.Money;
import org.springframework.data.jpa.domain.AbstractPersistable;

@Entity
@Table(name = "ct_savings_account_charge_schedule")
public class SavingsAccountChargeScheduleInstallment extends AbstractPersistable<Long> {
	
	@ManyToOne
    @JoinColumn(name = "savings_account_charge_id", nullable = false)
	private SavingsAccountCharge savingsAccountCharge;
	
	@Column(name = "due_amount", scale = 6, precision = 19, nullable = false)
    private BigDecimal dueAmount;
	
	@Column(name = "installment", nullable = false)
    private Integer installmentNumber;
	
	@Column(name = "paid_amount", scale = 6, precision = 19, nullable = false)
	private BigDecimal paidAmount;
	
	@Column(name = "waived_amount", scale = 6, precision = 19, nullable = false)
	private BigDecimal waivedAmount;
	
	@Temporal(TemporalType.DATE)
    @Column(name = "due_date", nullable = false)
    private Date dueDate;
	
	@Temporal(TemporalType.DATE)
    @Column(name = "obligations_met_on_date")
	private Date obligationsMetOnDate;
	
	@Column(name = "waived", nullable = false)
    private boolean waived = false;
	
	/**
     * @param savingsAccountCharge
     * @param installmentNumber
     * @param dueDate
     * @param dueAmount
     * @param paidAmount
     * @param waivedAmount
     * @param waived
     * @param obligationsMetOnDate
     */

	
	public static SavingsAccountChargeScheduleInstallment createNewWithoutSavingsAccountCharge
	(final Integer installmentNumber, final BigDecimal amount, final Date dueDate) {
        return new SavingsAccountChargeScheduleInstallment(null, installmentNumber, amount, dueDate, null);
    }
	
	public static SavingsAccountChargeScheduleInstallment installment(final SavingsAccountCharge savingsAccountCharge,
			final Integer installmentNumber, final Date dueDate, final BigDecimal dueAmount) {

        final Date obligationsMetOnDate = null;

        return new SavingsAccountChargeScheduleInstallment(savingsAccountCharge, installmentNumber, dueAmount, dueDate, 
        		obligationsMetOnDate);
    }
	
	protected SavingsAccountChargeScheduleInstallment() {
		//
    }
	
	private SavingsAccountChargeScheduleInstallment(final SavingsAccountCharge savingsAccountCharge,
			final Integer installmentNumber, final BigDecimal amount, final Date dueDate,
			final Date obligationsMetOnDate) {

        this.savingsAccountCharge = savingsAccountCharge;
        this.installmentNumber = installmentNumber;
        this.dueAmount = amount;
        this.dueDate = dueDate;
        this.paidAmount = null;
        this.waivedAmount = null;
        this.obligationsMetOnDate = obligationsMetOnDate;
        this.waived = false;
	}
	
	public Money undoPayment(final Money transactionAmountRemaining) {
		final MonetaryCurrency currency = transactionAmountRemaining.getCurrency();
		Money undonePaidAmountPortionOfTransaction = Money.zero(currency);
		final Money paidAmount = getPaidAmount(currency);
		if (transactionAmountRemaining.isGreaterThanOrEqualTo(paidAmount)) {
            this.paidAmount = Money.zero(currency).getAmount();
            undonePaidAmountPortionOfTransaction = paidAmount;
        } else {
            this.paidAmount = paidAmount.minus(transactionAmountRemaining).getAmount();
            undonePaidAmountPortionOfTransaction = transactionAmountRemaining;
        }
		
		this.paidAmount = defaultToNullIfZero(this.paidAmount);
		this.obligationsMetOnDate = null;
		
		return undonePaidAmountPortionOfTransaction;
	}
	
	public Money undoWaive(final Money transactionAmountRemaining) {
		final MonetaryCurrency currency = transactionAmountRemaining.getCurrency();
		Money undoneWaivedAmountPortionOfTransaction = Money.zero(currency);
		final Money waivedAmount = getWaivedAmount(currency);
		if (transactionAmountRemaining.isGreaterThanOrEqualTo(waivedAmount)) {
            this.waivedAmount = Money.zero(currency).getAmount();
            undoneWaivedAmountPortionOfTransaction = waivedAmount;
        } else {
            this.waivedAmount = waivedAmount.minus(transactionAmountRemaining).getAmount();
            undoneWaivedAmountPortionOfTransaction = transactionAmountRemaining;
        }
		
		this.waived = false;
		this.waivedAmount = defaultToNullIfZero(this.waivedAmount);
		this.obligationsMetOnDate = null;
		
		return undoneWaivedAmountPortionOfTransaction;
	}
	
	public Money payInstallment(final LocalDate transactionDate, final Money transactionAmountRemaining) {

        final MonetaryCurrency currency = transactionAmountRemaining.getCurrency();
        Money paidAmountPortionOfTransaction = Money.zero(currency);

        final Money overdueAmount = getInstallmentAmountOverdue(currency);
        if (transactionAmountRemaining.isGreaterThanOrEqualTo(overdueAmount)) {
            this.paidAmount = getPaidAmount(currency).plus(overdueAmount).getAmount();
            paidAmountPortionOfTransaction = paidAmountPortionOfTransaction.plus(overdueAmount);
        } else {
            this.paidAmount = getPaidAmount(currency).plus(transactionAmountRemaining).getAmount();
            paidAmountPortionOfTransaction = paidAmountPortionOfTransaction.plus(transactionAmountRemaining);
        }

        this.paidAmount = defaultToNullIfZero(this.paidAmount);

        checkIfInstallmentObligationsAreMet(transactionDate, currency);

        return paidAmountPortionOfTransaction;
    }
	
	private boolean checkIfInstallmentObligationsAreMet(final LocalDate transactionDate, final MonetaryCurrency currency) {
        boolean obligationsMet = getInstallmentAmountOverdue(currency).isZero();
        if (obligationsMet) {
            this.obligationsMetOnDate = transactionDate.toDate();
        }
        return obligationsMet;
    }
	
	public Money waive(final LocalDate transactionDate, final Money transactionAmountRemaining) {
		
		final MonetaryCurrency currency = transactionAmountRemaining.getCurrency();
        Money waivedAmountPortionOfTransaction = Money.zero(currency);

        final Money overdueAmount = getInstallmentAmountOverdue(currency);
        if (transactionAmountRemaining.isGreaterThanOrEqualTo(overdueAmount)) {
            this.waivedAmount = getWaivedAmount(currency).plus(overdueAmount).getAmount();
            waivedAmountPortionOfTransaction = waivedAmountPortionOfTransaction.plus(overdueAmount);
        } else {
            this.waivedAmount = getWaivedAmount(currency).plus(transactionAmountRemaining).getAmount();
            waivedAmountPortionOfTransaction = waivedAmountPortionOfTransaction.plus(transactionAmountRemaining);
        }

        this.waivedAmount = defaultToNullIfZero(this.waivedAmount);
        
        if(checkIfInstallmentObligationsAreMet(transactionDate, currency))
        		this.waived = true;
        
        return waivedAmountPortionOfTransaction;
    }
	
	public boolean isObligationsMet() {
        return (this.obligationsMetOnDate != null);
    }

    public boolean isNotFullyPaidOff() {
        return !(this.obligationsMetOnDate != null);
    }
    
    public LocalDate getDueDate() {
        return (this.dueDate == null) ? null : new LocalDate(this.dueDate);
    }
    
    public Money getInstallmentAmountOverdue(final MonetaryCurrency currency) {
        final Money accountedForAmount = getPaidAmount(currency).plus(getWaivedAmount(currency));
        return getDueAmount(currency).minus(accountedForAmount);
    }
    
    public Money getPaidAmount(final MonetaryCurrency currency) {
        return Money.of(currency, this.paidAmount);
    }
    
    public Money getWaivedAmount(final MonetaryCurrency currency) {
        return Money.of(currency, this.waivedAmount);
    }
    
    public Money getDueAmount(final MonetaryCurrency currency) {
        return Money.of(currency, this.dueAmount);
    }
    
    public void updateDueDate(final LocalDate newDueDate) {
        if (newDueDate != null) {
            this.dueDate = newDueDate.toDate();
        }
    }
    
    public void updateAmount(final BigDecimal dueAmount) {
        if (dueAmount != null) {
            this.dueAmount = dueAmount;
        }
    }
    
    private BigDecimal defaultToNullIfZero(final BigDecimal value) {
        BigDecimal result = value;
        if (BigDecimal.ZERO.compareTo(value) == 0) {
            result = null;
        }
        return result;
    }

}
