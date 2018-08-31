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
 * A simple data object that describes a deposit event.
 */
public class DepositEvent {

    private String accountId;

    private String bookEntryId;

    private long accountTransfer;

    private long bookEntryTransfer;

    /**
     * Creates a new DepositEvent.
     */
    public DepositEvent(
            String accountId,
            String bookEntryId,
            long accountTransfer,
            long bookEntryTransfer) {
        this.accountId = accountId;
        this.bookEntryId = bookEntryId;
        this.accountTransfer = accountTransfer;
        this.bookEntryTransfer = bookEntryTransfer;
    }

    public DepositEvent() {
    }


    // ------------------------------------------------------------------------
    //  properties
    // ------------------------------------------------------------------------

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public String getBookEntryId() {
        return bookEntryId;
    }

    public void setBookEntryId(String bookEntryId) {
        this.bookEntryId = bookEntryId;
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

    // ------------------------------------------------------------------------
    //  miscellaneous
    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "DepositEvent {"
                + "accountId=" + accountId
                + ", bookEntryId=" + bookEntryId
                + ", accountTransfer=" + accountTransfer
                + ", bookEntryTransfer=" + bookEntryTransfer
                + '}';
    }
}
