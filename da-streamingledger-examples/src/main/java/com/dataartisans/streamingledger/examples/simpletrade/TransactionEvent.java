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

/**
 * A simple data object that describes a transaction.
 */
public class TransactionEvent {

    private String sourceAccountId;

    private String targetAccountId;

    private String sourceBookEntryId;

    private String targetBookEntryId;

    private long accountTransfer;

    private long bookEntryTransfer;

    private long minAccountBalance;

    /**
     * Creates a new TransactionEvent for the given accounts and book entries.
     */
    public TransactionEvent(
            String sourceAccountId,
            String targetAccountId,
            String sourceBookEntryId,
            String targetBookEntryId,
            long accountTransfer,
            long bookEntryTransfer,
            long minAccountBalance) {

        this.sourceAccountId = sourceAccountId;
        this.targetAccountId = targetAccountId;
        this.sourceBookEntryId = sourceBookEntryId;
        this.targetBookEntryId = targetBookEntryId;
        this.accountTransfer = accountTransfer;
        this.bookEntryTransfer = bookEntryTransfer;
        this.minAccountBalance = minAccountBalance;
    }

    public TransactionEvent() {
    }

    // ------------------------------------------------------------------------
    //  properties
    // ------------------------------------------------------------------------

    public String getSourceAccountId() {
        return sourceAccountId;
    }

    public void setSourceAccountId(String sourceAccountId) {
        this.sourceAccountId = sourceAccountId;
    }

    public String getTargetAccountId() {
        return targetAccountId;
    }

    public void setTargetAccountId(String targetAccountId) {
        this.targetAccountId = targetAccountId;
    }

    public String getSourceBookEntryId() {
        return sourceBookEntryId;
    }

    public void setSourceBookEntryId(String sourceBookEntryId) {
        this.sourceBookEntryId = sourceBookEntryId;
    }

    public String getTargetBookEntryId() {
        return targetBookEntryId;
    }

    public void setTargetBookEntryId(String targetBookEntryId) {
        this.targetBookEntryId = targetBookEntryId;
    }

    public long getAccountTransfer() {
        return accountTransfer;
    }

    public void setAccountTransfer(long accountTransfer) {
        this.accountTransfer = accountTransfer;
    }

    public long getBookEntryTransfer() {
        return bookEntryTransfer;
    }

    public void setBookEntryTransfer(long bookEntryTransfer) {
        this.bookEntryTransfer = bookEntryTransfer;
    }

    public long getMinAccountBalance() {
        return minAccountBalance;
    }

    public void setMinAccountBalance(long minAccountBalance) {
        this.minAccountBalance = minAccountBalance;
    }

    // ------------------------------------------------------------------------
    //  miscellaneous
    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "TransactionEvent {"
                + "sourceAccountId=" + sourceAccountId
                + ", targetAccountId=" + targetAccountId
                + ", sourceBookEntryId=" + sourceBookEntryId
                + ", targetBookEntryId=" + targetBookEntryId
                + ", accountTransfer=" + accountTransfer
                + ", bookEntryTransfer=" + bookEntryTransfer
                + ", minAccountBalance=" + minAccountBalance
                + '}';
    }
}
