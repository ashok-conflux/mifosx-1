package org.mifosplatform.infrastructure.jobs.service;

public enum JobName {

    UPDATE_LOAN_SUMMARY(1l, "Update loan Summary"), //
    UPDATE_LOAN_ARREARS_AGEING(2l, "Update Loan Arrears Ageing"), //
    UPDATE_LOAN_PAID_IN_ADVANCE(3l, "Update Loan Paid In Advance"), //
    APPLY_ANNUAL_FEE_FOR_SAVINGS(4l, "Apply Annual Fee For Savings"), //
    APPLY_HOLIDAYS_TO_LOANS(5l, "Apply Holidays To Loans"), //
    POST_INTEREST_FOR_SAVINGS(6l, "Post Interest For Savings"), //
    TRANSFER_FEE_CHARGE_FOR_LOANS(7l, "Transfer Fee For Loans From Savings"), //
    PAY_DUE_SAVINGS_CHARGES(8l, "Pay Due Savings Charges"), //
    ACCOUNTING_RUNNING_BALANCE_UPDATE(9l, "Update Accounting Running Balances"), //
    EXECUTE_STANDING_INSTRUCTIONS(10l, "Execute Standing Instruction"),
    ADD_ACCRUAL_ENTRIES(11l, "Add Accrual Transactions"),
    APPLY_CHARGE_TO_OVERDUE_LOAN_INSTALLMENT(12l, "Apply penalty to overdue loans"),
    UPDATE_NPA(13l, "Update Non Performing Assets"),
    TRANSFER_INTEREST_TO_SAVINGS(14l, "Transfer Interest To Savings"),
    UPDATE_DEPOSITS_ACCOUNT_MATURITY_DETAILS(15l, "Update Deposit Accounts Maturity details"),
    ADD_PERIODIC_ACCRUAL_ENTRIES(16l, "Add Periodic Accrual Transactions"),
    UPDATE_CALENDAR_DATES(17l, "Update Calendar Dates"),
    UPDATE_CHARGE_INSTALLMENT_DATES(18l, "Update Charge Installment Dates");

    private final String name;
    
    private final Long value;

    private JobName(final Long value, final String name) {
        this.name = name;
        this.value = value;
    }

    @Override
    public String toString() {
        return this.name;
    }

	public Long getValue() {
		return value;
	}
}
