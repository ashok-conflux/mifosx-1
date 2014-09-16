/**
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.mifosplatform.portfolio.savings.domain;

import java.util.Date;
import java.util.List;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface SavingsAccountChargeRepository extends JpaRepository<SavingsAccountCharge, Long>,
        JpaSpecificationExecutor<SavingsAccountCharge> {

    SavingsAccountCharge findByIdAndSavingsAccountId(Long id, Long savingsAccountId);

    @Query("from SavingsAccountCharge sac where sac.dueDate <=:transactionDate and sac.waived = 0 and sac.paid=0 order by sac.dueDate")
    List<SavingsAccountCharge> findPendingCharges(@Param("transactionDate") Date transactionDate);
    
    @Query("from SavingsAccountCharge sac where sac.status = 1 and sac.dueDate < curdate() and (sac.chargeTime = 6 or sac.chargeTime = 7 or sac.chargeTime = 11)")
    Page<SavingsAccountCharge> findChargesRequiringUpdate(Pageable pageable);
    
}
