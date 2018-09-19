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

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Either;

import com.dataartisans.streamingledger.examples.simpletrade.DepositEvent;
import com.dataartisans.streamingledger.examples.simpletrade.TransactionEvent;

import java.util.SplittableRandom;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A random data generator with data rate throttling logic.
 *
 * <p>This source emits two kinds of events {@link DepositEvent} and a {@link TransactionEvent}. First this source emits
 * deposit events for each account and book entry, and then starts emitting random transaction events while not
 * canceled.
 */
final class DepositsThenTransactionsSource extends RichParallelSourceFunction<Either<DepositEvent, TransactionEvent>> {

    private static final int NUM_ROWS = 1_000_000;
    private static final String ACCOUNT_ID_PREFIX = "ACCT-";
    private static final String BOOK_ENTRY_ID_PREFIX = "BOOK-";
    private static final long MAX_ACCOUNT_TRANSFER = 10_000;
    private static final long MAX_BOOK_TRANSFER = 1_000;
    private static final long MIN_BALANCE = 0;

    private static final long serialVersionUID = 1L;

    private final int maxRecordsPerSecond;

    private volatile boolean running = true;

    DepositsThenTransactionsSource(int maxRecordsPerSecond) {
        checkArgument(maxRecordsPerSecond == -1 || maxRecordsPerSecond > 0,
                "maxRecordsPerSecond must be positive or -1 (infinite)");
        this.maxRecordsPerSecond = maxRecordsPerSecond;
    }

    @Override
    public void run(SourceContext<Either<DepositEvent, TransactionEvent>> context) throws Exception {
        final int numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
        final int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();

        collectDeposits(context, indexOfThisSubtask, numberOfParallelSubtasks);
        collectTransactions(context, numberOfParallelSubtasks);
    }

    @Override
    public void cancel() {
        running = false;
    }

    private void collectDeposits(
            SourceContext<Either<DepositEvent, TransactionEvent>> context,
            final int indexOfThisSubtask,
            final int numberOfParallelSubtasks) {

        final int startId = (indexOfThisSubtask * NUM_ROWS) / numberOfParallelSubtasks;
        final int endId = ((indexOfThisSubtask + 1) * NUM_ROWS) / numberOfParallelSubtasks;

        for (int i = startId; i < endId; i++) {
            String accountId = ACCOUNT_ID_PREFIX + i;
            String bookEntryId = BOOK_ENTRY_ID_PREFIX + i;

            DepositEvent event = new DepositEvent(
                    accountId,
                    bookEntryId,
                    MAX_ACCOUNT_TRANSFER,
                    MAX_BOOK_TRANSFER);

            context.collect(Either.Left(event));
        }
    }

    private void collectTransactions(
            SourceContext<Either<DepositEvent, TransactionEvent>> context,
            int numberOfParallelSubtasks) throws InterruptedException {

        SplittableRandom random = new SplittableRandom();
        Throttler throttler = new Throttler(maxRecordsPerSecond, numberOfParallelSubtasks);
        while (running) {
            TransactionEvent event = randomTransactionEvent(random);
            context.collect(Either.Right(event));
            throttler.throttle();
        }
    }

    private TransactionEvent randomTransactionEvent(SplittableRandom rnd) {
        final long accountsTransfer = rnd.nextLong(MAX_ACCOUNT_TRANSFER);
        final long transfer = rnd.nextLong(MAX_BOOK_TRANSFER);
        while (true) {
            final int sourceAcct = rnd.nextInt(NUM_ROWS);
            final int targetAcct = rnd.nextInt(NUM_ROWS);
            final int sourceBook = rnd.nextInt(NUM_ROWS);
            final int targetBook = rnd.nextInt(NUM_ROWS);

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
