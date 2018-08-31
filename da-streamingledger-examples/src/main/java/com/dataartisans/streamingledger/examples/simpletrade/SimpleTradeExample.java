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

import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import com.dataartisans.streamingledger.examples.simpletrade.generator.DepositsGenerator;
import com.dataartisans.streamingledger.examples.simpletrade.generator.TransactionsGenerator;
import com.dataartisans.streamingledger.sdk.api.StateAccess;
import com.dataartisans.streamingledger.sdk.api.StreamingLedger;
import com.dataartisans.streamingledger.sdk.api.StreamingLedger.ResultStreams;
import com.dataartisans.streamingledger.sdk.api.TransactionProcessFunction;

import java.net.URI;
import java.nio.file.Paths;
import java.util.function.Supplier;

import static com.dataartisans.streamingledger.sdk.api.AccessType.READ_WRITE;

/**
 * A simple example illustrating the use of stream ledger.
 *
 * <p>The example here uses two states (called "accounts" and "bookEntries") and modifies two keys in each state in one
 * joint transaction.
 */
public class SimpleTradeExample {

    private static final Supplier<Long> ZERO = () -> 0L;

    /**
     * The main entry point to the sample application. This runs the program with a
     * built-in data generator and the non-parallel local runtime implementation for
     * the transaction logic.
     *
     * @param args The command line arguments.
     */
    public static void main(String[] args) throws Exception {

        // set up the execution environment and the configuration
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure Flink
        env.setParallelism(4);
        env.getConfig().enableObjectReuse();

        // enable checkpoints once a minute
        env.enableCheckpointing(60_000);
        URI uri = Paths.get("./checkpoints").toAbsolutePath().normalize().toUri();
        StateBackend backend = new FsStateBackend(uri, true);
        env.setStateBackend(backend);

        // start building the transactional streams
        StreamingLedger tradeLedger = StreamingLedger.create("simple trade example");

        // define the transactional states
        StreamingLedger.State<String, Long> accounts = tradeLedger.declareState("accounts")
                .withKeyType(String.class)
                .withValueType(Long.class);

        StreamingLedger.State<String, Long> books = tradeLedger.declareState("bookEntries")
                .withKeyType(String.class)
                .withValueType(Long.class);

        // produce the deposits transaction stream
        DataStream<DepositEvent> deposits = env.addSource(new DepositsGenerator(1));

        // define transactors on states
        tradeLedger.usingStream(deposits, "deposits")
                .apply(new DepositHandler())
                .on(accounts, DepositEvent::getAccountId, "account", READ_WRITE)
                .on(books, DepositEvent::getBookEntryId, "asset", READ_WRITE);

        // produce transactions stream
        DataStream<TransactionEvent> transfers = env.addSource(new TransactionsGenerator(1));

        OutputTag<TransactionResult> transactionResults = tradeLedger.usingStream(transfers, "transactions")
                .apply(new TxnHandler())
                .on(accounts, TransactionEvent::getSourceAccountId, "source-account", READ_WRITE)
                .on(accounts, TransactionEvent::getTargetAccountId, "target-account", READ_WRITE)
                .on(books, TransactionEvent::getSourceBookEntryId, "source-asset", READ_WRITE)
                .on(books, TransactionEvent::getTargetBookEntryId, "target-asset", READ_WRITE)
                .output();

        //  compute the resulting streams.
        ResultStreams resultsStreams = tradeLedger.resultStreams();

        // output to the console
        resultsStreams.getResultStream(transactionResults).print();

        // trigger program execution
        env.execute();
    }

    /**
     * The implementation of the logic that executes a deposit.
     */
    private static final class DepositHandler extends TransactionProcessFunction<DepositEvent, Void> {

        private static final long serialVersionUID = 1;

        @ProcessTransaction
        public void process(
                final DepositEvent event,
                final Context<Void> ctx,
                final @State("account") StateAccess<Long> account,
                final @State("asset") StateAccess<Long> asset) {

            long newAccountValue = account.readOr(ZERO) + event.getAccountTransfer();

            account.write(newAccountValue);

            long newAssetValue = asset.readOr(ZERO) + event.getBookEntryTransfer();
            asset.write(newAssetValue);
        }
    }

    /**
     * The implementation of the logic that executes the transaction. The logic is given the original
     * TransactionEvent plus all states involved in the transaction.
     */
    private static final class TxnHandler extends TransactionProcessFunction<TransactionEvent, TransactionResult> {

        private static final long serialVersionUID = 1;

        @ProcessTransaction
        public void process(
                final TransactionEvent txn,
                final Context<TransactionResult> ctx,
                final @State("source-account") StateAccess<Long> sourceAccount,
                final @State("target-account") StateAccess<Long> targetAccount,
                final @State("source-asset") StateAccess<Long> sourceAsset,
                final @State("target-asset") StateAccess<Long> targetAsset) {

            final long sourceAccountBalance = sourceAccount.readOr(ZERO);
            final long sourceAssetValue = sourceAsset.readOr(ZERO);
            final long targetAccountBalance = targetAccount.readOr(ZERO);
            final long targetAssetValue = targetAsset.readOr(ZERO);

            // check the preconditions
            if (sourceAccountBalance > txn.getMinAccountBalance()
                    && sourceAccountBalance > txn.getAccountTransfer()
                    && sourceAssetValue > txn.getBookEntryTransfer()) {

                // compute the new balances
                final long newSourceBalance = sourceAccountBalance - txn.getAccountTransfer();
                final long newTargetBalance = targetAccountBalance + txn.getAccountTransfer();
                final long newSourceAssets = sourceAssetValue - txn.getBookEntryTransfer();
                final long newTargetAssets = targetAssetValue + txn.getBookEntryTransfer();

                // write back the updated values
                sourceAccount.write(newSourceBalance);
                targetAccount.write(newTargetBalance);
                sourceAsset.write(newSourceAssets);
                targetAsset.write(newTargetAssets);

                // emit result event with updated balances and flag to mark transaction as processed
                ctx.emit(new TransactionResult(txn, true, newSourceBalance, newTargetBalance));
            }
            else {
                // emit result with unchanged balances and a flag to mark transaction as rejected
                ctx.emit(new TransactionResult(txn, false, sourceAccountBalance, targetAccountBalance));
            }
        }
    }

}
