/**
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.mifosplatform.portfolio.collectionsheet.data;

import java.util.Collection;
import java.util.List;

import org.joda.time.LocalDate;
import org.mifosplatform.infrastructure.core.data.EnumOptionData;
import org.mifosplatform.portfolio.loanproduct.data.LoanProductData;
import org.mifosplatform.portfolio.savings.data.SavingsProductData;

/**
 * Immutable data object for join liability group's collection sheet.
 */
public class JLGCollectionSheetData {

    private final LocalDate dueDate;
    private final Collection<LoanProductData> loanProductsDueForRepayment;
    private final Collection<LoanProductData> loanProductsDueForDisbursal;
    @SuppressWarnings("unused")
    private final Collection<SavingsProductData> savingsProducts;
    private final Collection<JLGGroupData> groups;
    private final List<EnumOptionData> attendanceTypeOptions;

    public static JLGCollectionSheetData instance(final LocalDate date, final Collection<LoanProductData> loanProductsDueForRepayment,
    		final Collection<LoanProductData> loanProductsDueForDisbursal, final Collection<JLGGroupData> groups, 
    		final List<EnumOptionData> attendanceTypeOptions) {
    	
        return new JLGCollectionSheetData(date, loanProductsDueForRepayment, loanProductsDueForDisbursal, null, groups, attendanceTypeOptions);
    }

    public static JLGCollectionSheetData withSavingsProducts(final JLGCollectionSheetData data,
            final Collection<SavingsProductData> savingsProducts) {

        return new JLGCollectionSheetData(data.dueDate, data.loanProductsDueForRepayment, data.loanProductsDueForDisbursal, 
        		savingsProducts, data.groups, data.attendanceTypeOptions);
    }

    private JLGCollectionSheetData(LocalDate dueDate, Collection<LoanProductData> loanProductsDueForRepayment, 
    		Collection<LoanProductData> loanProductsDueForDisbursal, Collection<SavingsProductData> savingsProducts, 
    		Collection<JLGGroupData> groups, List<EnumOptionData> attendanceTypeOptions) {
        this.dueDate = dueDate;
        this.loanProductsDueForRepayment = loanProductsDueForRepayment;
        this.loanProductsDueForDisbursal = loanProductsDueForDisbursal; 
        this.savingsProducts = savingsProducts;
        this.groups = groups;
        this.attendanceTypeOptions = attendanceTypeOptions;
    }

    public LocalDate getDate() {
        return this.dueDate;
    }

    public Collection<JLGGroupData> getGroups() {
        return this.groups;
    }

    public Collection<LoanProductData> getLoanProducts() {
        return this.loanProductsDueForRepayment;
    }

}