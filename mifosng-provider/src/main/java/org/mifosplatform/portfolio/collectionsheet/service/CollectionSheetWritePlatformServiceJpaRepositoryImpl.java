/**
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.mifosplatform.portfolio.collectionsheet.service;

import static org.mifosplatform.portfolio.collectionsheet.CollectionSheetConstants.bulkSavingsDueTransactionsParamName;
import static org.mifosplatform.portfolio.collectionsheet.CollectionSheetConstants.bulkSavingsWithdrawalTransactionsParamName;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.mifosplatform.infrastructure.core.api.JsonCommand;
import org.mifosplatform.infrastructure.core.data.CommandProcessingResult;
import org.mifosplatform.infrastructure.core.data.CommandProcessingResultBuilder;
import org.mifosplatform.portfolio.collectionsheet.command.CollectionSheetBulkDisbursalCommand;
import org.mifosplatform.portfolio.collectionsheet.command.CollectionSheetBulkRepaymentCommand;
import org.mifosplatform.portfolio.collectionsheet.data.CollectionSheetTransactionDataValidator;
import org.mifosplatform.portfolio.collectionsheet.serialization.CollectionSheetBulkDisbursalCommandFromApiJsonDeserializer;
import org.mifosplatform.portfolio.collectionsheet.serialization.CollectionSheetBulkRepaymentCommandFromApiJsonDeserializer;
import org.mifosplatform.portfolio.loanaccount.service.LoanWritePlatformService;
import org.mifosplatform.portfolio.meeting.service.MeetingWritePlatformService;
import org.mifosplatform.portfolio.savings.data.SavingsAccountTransactionDTO;
import org.mifosplatform.portfolio.savings.domain.DepositAccountAssembler;
import org.mifosplatform.portfolio.savings.domain.SavingsAccount;
import org.mifosplatform.portfolio.savings.domain.SavingsAccountRepository;
import org.mifosplatform.portfolio.savings.service.DepositAccountWritePlatformService;
import org.mifosplatform.portfolio.savings.service.SavingsAccountWritePlatformService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class CollectionSheetWritePlatformServiceJpaRepositoryImpl implements CollectionSheetWritePlatformService {

	private final static Logger logger = LoggerFactory.getLogger(CollectionSheetWritePlatformServiceJpaRepositoryImpl.class);
    private final LoanWritePlatformService loanWritePlatformService;
    private final CollectionSheetBulkRepaymentCommandFromApiJsonDeserializer bulkRepaymentCommandFromApiJsonDeserializer;
    private final CollectionSheetBulkDisbursalCommandFromApiJsonDeserializer bulkDisbursalCommandFromApiJsonDeserializer;
    private final CollectionSheetTransactionDataValidator transactionDataValidator;
    private final MeetingWritePlatformService meetingWritePlatformService;
    private final DepositAccountAssembler accountAssembler;
    private final DepositAccountWritePlatformService accountWritePlatformService;
    private final SavingsAccountWritePlatformService savingsAccountWritePlatformService;
    private final SavingsAccountRepository savingsRepository;

    @Autowired
    public CollectionSheetWritePlatformServiceJpaRepositoryImpl(final LoanWritePlatformService loanWritePlatformService,
            final CollectionSheetBulkRepaymentCommandFromApiJsonDeserializer bulkRepaymentCommandFromApiJsonDeserializer,
            final CollectionSheetBulkDisbursalCommandFromApiJsonDeserializer bulkDisbursalCommandFromApiJsonDeserializer,
            final CollectionSheetTransactionDataValidator transactionDataValidator,
            final MeetingWritePlatformService meetingWritePlatformService, final DepositAccountAssembler accountAssembler,
            final DepositAccountWritePlatformService accountWritePlatformService, final SavingsAccountRepository savingsRepository,
            final SavingsAccountWritePlatformService savingsAccountWritePlatformService) {
        this.loanWritePlatformService = loanWritePlatformService;
        this.bulkRepaymentCommandFromApiJsonDeserializer = bulkRepaymentCommandFromApiJsonDeserializer;
        this.bulkDisbursalCommandFromApiJsonDeserializer = bulkDisbursalCommandFromApiJsonDeserializer;
        this.transactionDataValidator = transactionDataValidator;
        this.meetingWritePlatformService = meetingWritePlatformService;
        this.accountAssembler = accountAssembler;
        this.accountWritePlatformService = accountWritePlatformService;
        this.savingsRepository = savingsRepository;
        this.savingsAccountWritePlatformService = savingsAccountWritePlatformService;
    }

    @Override
    public CommandProcessingResult updateCollectionSheet(final JsonCommand command) {

        this.transactionDataValidator.validateTransaction(command);

        final Map<String, Object> changes = new HashMap<>();
        changes.put("locale", command.locale());
        changes.put("dateFormat", command.dateFormat());

        final String noteText = command.stringValueOfParameterNamed("note");
        if (StringUtils.isNotBlank(noteText)) {
            changes.put("note", noteText);
        }

        changes.putAll(updateBulkRepayments(command));
        changes.putAll(updateBulkDisbursals(command));
        changes.putAll(updateBulkDepositPayments(command));
        changes.putAll(updateBulkWithdrawalPayments(command));
        

        this.meetingWritePlatformService.updateCollectionSheetAttendance(command);

        return new CommandProcessingResultBuilder() //
                .withCommandId(command.commandId()) //
                .withEntityId(command.entityId()) //
                .withGroupId(command.entityId()) //
                .with(changes).with(changes).build();
    }

    private Map<String, Object> updateBulkRepayments(final JsonCommand command) {
        final Map<String, Object> changes = new HashMap<>();
        final CollectionSheetBulkRepaymentCommand bulkRepaymentCommand = this.bulkRepaymentCommandFromApiJsonDeserializer
                .commandFromApiJson(command.json());
        changes.putAll(this.loanWritePlatformService.makeLoanBulkRepayment(bulkRepaymentCommand));
        return changes;
    }
    
    private Map<String, Object> updateBulkDisbursals(final JsonCommand command) {
        final Map<String, Object> changes = new HashMap<>();
        final CollectionSheetBulkDisbursalCommand bulkDisbursalCommand = this.bulkDisbursalCommandFromApiJsonDeserializer
                .commandFromApiJson(command.json());
        changes.putAll(this.loanWritePlatformService.bulkLoanDisbursal(command, bulkDisbursalCommand, false));
        return changes;
    }

    private Map<String, Object> updateBulkDepositPayments(final JsonCommand command) {
    	
        final Map<String, Object> changes = new HashMap<>();
        final Collection<SavingsAccountTransactionDTO> savingsTransactions = this.accountAssembler.assembleBulkMandatorySavingsAccountTransactionDTOs(command,
        		bulkSavingsDueTransactionsParamName);
        
        final Locale locale = command.extractLocale();
        final DateTimeFormatter fmt = DateTimeFormat.forPattern(command.dateFormat()).withLocale(locale);
        
        for (SavingsAccountTransactionDTO savingsAccountTransactionDTO : savingsTransactions) {
        	
        	if(savingsAccountTransactionDTO != null){
        		if(savingsAccountTransactionDTO.getTransactionAmount().compareTo(BigDecimal.ZERO)> 0){
		            	// TODO instead of repository use read platform service
		            	SavingsAccount savingsAccount = savingsRepository.findById(savingsAccountTransactionDTO.getSavingsAccountId());
		            	if(savingsAccount.isRDAccount() && savingsAccount.isMandatoryDeposit()){
		                    this.accountWritePlatformService.mandatorySavingsAccountDeposit(savingsAccountTransactionDTO);
		                    changes.put("savingsAccountId", savingsAccountTransactionDTO.getSavingsAccountId());
		                    changes.put("transactionAmount", savingsAccountTransactionDTO.getTransactionAmount());
		            	}
		            	else if(savingsAccount.isSavingsAccount()){
		            		this.savingsAccountWritePlatformService.deposit(savingsAccountTransactionDTO, fmt);
		            		changes.put("savingsAccountId", savingsAccountTransactionDTO.getSavingsAccountId());
		                    changes.put("transactionAmount", savingsAccountTransactionDTO.getTransactionAmount());
		            	}
		            	else{
		            		
		            		// TODO throw exception saying not supported deposit/saving account type in bulk entry
		            		logger.error("Deposit entry in bulk entry mode is allowed only for savings and Mandatory RD accounts");
		            	}
        		}
        	}
        }
        
        return changes;
    }

	private Map<String, Object> updateBulkWithdrawalPayments(
			final JsonCommand command) {
		final Map<String, Object> changes = new HashMap<>();
		final Collection<SavingsAccountTransactionDTO> savingsTransactions = this.accountAssembler
				.assembleBulkMandatorySavingsAccountTransactionDTOs(command,
						bulkSavingsWithdrawalTransactionsParamName);

		final Locale locale = command.extractLocale();
		final DateTimeFormatter fmt = DateTimeFormat.forPattern(
				command.dateFormat()).withLocale(locale);

		for (SavingsAccountTransactionDTO savingsAccountTransactionDTO : savingsTransactions) {

			if (savingsAccountTransactionDTO != null) {
				if (savingsAccountTransactionDTO.getTransactionAmount()
						.compareTo(BigDecimal.ZERO) > 0) {
						// TODO instead of repository use read platform service
						SavingsAccount savingsAccount = savingsRepository
								.findById(savingsAccountTransactionDTO
										.getSavingsAccountId());
						if (savingsAccount.isSavingsAccount()) {
							this.savingsAccountWritePlatformService.withdraw(
									savingsAccountTransactionDTO, fmt);
							changes.put("savingsAccountId",
									savingsAccountTransactionDTO
											.getSavingsAccountId());
							changes.put("transactionAmount",
									savingsAccountTransactionDTO
											.getTransactionAmount());
						} else if (savingsAccount.isRDAccount()) {
							this.accountWritePlatformService
									.withdraw(savingsAccountTransactionDTO);
							changes.put("savingsAccountId",
									savingsAccountTransactionDTO
											.getSavingsAccountId());
							changes.put("transactionAmount",
									savingsAccountTransactionDTO
											.getTransactionAmount());
						} else {
		            		
		            		// TODO throw exception saying not supported deposit/saving account type in bulk entry
		            		logger.error("Withdrawal entry in bulk entry mode is allowed only for savings and withdrawal-enabled RD accounts");
		            	}
				}
			}
		}

		return changes;
	}
}
