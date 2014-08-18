package org.mifosplatform.scheduledjobs.data;

import java.math.BigDecimal;

import org.joda.time.LocalDate;

public class FutureChargeScheduleInstallment {

	private final Long officeId;
	private final Long savingsAccountChargeId;
	private final String recurrence;
	private final LocalDate startDate;
	private final LocalDate fromDate;
	private final Long lastInstallmentNumber;
	private final int numberOfFutureMeetings;
	private final BigDecimal dueAmount;
	
	

	public FutureChargeScheduleInstallment(final Long officeId, final Long savingsAccountChargeId,
			final int numberOfFutureMeetings, final LocalDate fromDate, final Long lastInstallmentNumber,
			final BigDecimal dueAmount, final LocalDate startDate,
			final String recurrence) {
		this.officeId = officeId;
		this.savingsAccountChargeId = savingsAccountChargeId;
		this.recurrence = recurrence;
		this.startDate = startDate;
		this.fromDate = fromDate;
		this.lastInstallmentNumber = lastInstallmentNumber;
		this.numberOfFutureMeetings = numberOfFutureMeetings;
		this.dueAmount = dueAmount;
		
	}

	public int getNumberOfFutureMeetings() {
		return numberOfFutureMeetings;
	}

	public LocalDate getFromDate() {
		return fromDate;
	}

	public Long getLastInstallmentNumber() {
		return lastInstallmentNumber;
	}

	public String getRecurrence() {
		return recurrence;
	}

	public LocalDate getStartDate() {
		return startDate;
	}

	public Long getOfficeId() {
		return officeId;
	}

	public Long getSavingsAccountChargeId() {
		return savingsAccountChargeId;
	}

	public BigDecimal getDueAmount() {
		return dueAmount;
	}
   
}

