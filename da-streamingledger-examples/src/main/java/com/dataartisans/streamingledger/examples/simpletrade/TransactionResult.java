/*
 *  Copyright 2018 Data Artisans GmbH
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.dataartisans.streamingledger.examples.simpletrade;

import static java.util.Objects.requireNonNull;

/**
 * Data type describing the result of a processed transaction.
 * It describes whether the transaction was successful as well as the resulting account balances.
 */
public class TransactionResult {

    private TransactionEvent transaction;

    private boolean success;

    private long newSourceAccountBalance;

    private long newTargetAccountBalance;

    /**
     * Creates a new transaction result.
     *
     * @param transaction             The original transaction event.
     * @param success                 True, if the transaction was successful, false if not.
     * @param newSourceAccountBalance The resulting balance of the source account.
     * @param newTargetAccountBalance The resulting balance of the target account.
     */
    public TransactionResult(
            TransactionEvent transaction,
            boolean success,
            long newSourceAccountBalance,
            long newTargetAccountBalance) {

        this.transaction = requireNonNull(transaction);
        this.success = success;
        this.newSourceAccountBalance = newSourceAccountBalance;
        this.newTargetAccountBalance = newTargetAccountBalance;
    }

    public TransactionResult() {
    }

    // ------------------------------------------------------------------------
    //  Properties
    // ------------------------------------------------------------------------

    public TransactionEvent getTransaction() {
        return transaction;
    }

    public void setTransaction(TransactionEvent transaction) {
        this.transaction = transaction;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public long getNewSourceAccountBalance() {
        return newSourceAccountBalance;
    }

    public void setNewSourceAccountBalance(long newSourceAccountBalance) {
        this.newSourceAccountBalance = newSourceAccountBalance;
    }

    public long getNewTargetAccountBalance() {
        return newTargetAccountBalance;
    }

    public void setNewTargetAccountBalance(long newTargetAccountBalance) {
        this.newTargetAccountBalance = newTargetAccountBalance;
    }

    // ------------------------------------------------------------------------
    //  Miscellaneous
    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "TransactionResult {"
                + "transaction=" + transaction
                + ", success=" + success
                + ", newSourceAccountBalance=" + newSourceAccountBalance
                + ", newTargetAccountBalance=" + newTargetAccountBalance
                + '}';
    }
}
