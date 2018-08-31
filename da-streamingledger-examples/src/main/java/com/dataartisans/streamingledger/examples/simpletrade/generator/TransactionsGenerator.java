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

package com.dataartisans.streamingledger.examples.simpletrade.generator;

import com.dataartisans.streamingledger.examples.simpletrade.TransactionEvent;

import java.util.SplittableRandom;

import static com.dataartisans.streamingledger.examples.simpletrade.generator.Constants.ACCOUNT_ID_PREFIX;
import static com.dataartisans.streamingledger.examples.simpletrade.generator.Constants.BOOK_ENTRY_ID_PREFIX;
import static com.dataartisans.streamingledger.examples.simpletrade.generator.Constants.MAX_ACCOUNT_TRANSFER;
import static com.dataartisans.streamingledger.examples.simpletrade.generator.Constants.MAX_BOOK_TRANSFER;
import static com.dataartisans.streamingledger.examples.simpletrade.generator.Constants.MIN_BALANCE;
import static com.dataartisans.streamingledger.examples.simpletrade.generator.Constants.NUM_ACCOUNTS;
import static com.dataartisans.streamingledger.examples.simpletrade.generator.Constants.NUM_BOOK_ENTRIES;

/**
 * A {@link TransactionEvent} generator.
 */
public final class TransactionsGenerator extends BaseGenerator<TransactionEvent> {

    private static final long serialVersionUID = 1L;

    public TransactionsGenerator(int maxRecordsPerSecond) {
        super(maxRecordsPerSecond);
    }

    protected TransactionEvent randomEvent(SplittableRandom rnd) {
        final long accountsTransfer = rnd.nextLong(MAX_ACCOUNT_TRANSFER);
        final long transfer = rnd.nextLong(MAX_BOOK_TRANSFER);
        while (true) {
            final int sourceAcct = rnd.nextInt(NUM_ACCOUNTS);
            final int targetAcct = rnd.nextInt(NUM_ACCOUNTS);
            final int sourceBook = rnd.nextInt(NUM_BOOK_ENTRIES);
            final int targetBook = rnd.nextInt(NUM_BOOK_ENTRIES);

            if (sourceAcct == targetAcct || sourceBook == targetBook) {
                continue;
            }
            return new TransactionEvent(
                    ACCOUNT_ID_PREFIX + sourceAcct,
                    ACCOUNT_ID_PREFIX + targetAcct,
                    BOOK_ENTRY_ID_PREFIX + sourceBook,
                    BOOK_ENTRY_ID_PREFIX + targetBook,
                    accountsTransfer,
                    transfer,
                    MIN_BALANCE);
        }
    }
}
