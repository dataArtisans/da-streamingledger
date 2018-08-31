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

import com.dataartisans.streamingledger.examples.simpletrade.DepositEvent;

import java.util.SplittableRandom;

import static com.dataartisans.streamingledger.examples.simpletrade.generator.Constants.ACCOUNT_ID_PREFIX;
import static com.dataartisans.streamingledger.examples.simpletrade.generator.Constants.BOOK_ENTRY_ID_PREFIX;
import static com.dataartisans.streamingledger.examples.simpletrade.generator.Constants.MAX_ACCOUNT_TRANSFER;
import static com.dataartisans.streamingledger.examples.simpletrade.generator.Constants.MAX_BOOK_TRANSFER;
import static com.dataartisans.streamingledger.examples.simpletrade.generator.Constants.NUM_ACCOUNTS;
import static com.dataartisans.streamingledger.examples.simpletrade.generator.Constants.NUM_BOOK_ENTRIES;

/**
 * A {@link DepositEvent} generator.
 */
public final class DepositsGenerator extends BaseGenerator<DepositEvent> {

    private static final long serialVersionUID = 1L;

    public DepositsGenerator(int maxRecordsPerSecond) {
        super(maxRecordsPerSecond);
    }

    @Override
    protected DepositEvent randomEvent(SplittableRandom rnd) {
        final int account = rnd.nextInt(NUM_ACCOUNTS);
        final int book = rnd.nextInt(NUM_BOOK_ENTRIES);
        final long accountsDeposit = rnd.nextLong(MAX_ACCOUNT_TRANSFER);
        final long deposit = rnd.nextLong(MAX_BOOK_TRANSFER);

        return new DepositEvent(
                ACCOUNT_ID_PREFIX + account,
                BOOK_ENTRY_ID_PREFIX + book,
                accountsDeposit,
                deposit);
    }
}
