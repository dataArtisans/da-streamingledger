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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import com.dataartisans.streamingledger.examples.simpletrade.DepositEvent;
import com.dataartisans.streamingledger.examples.simpletrade.TransactionEvent;

/**
 * Creates two synthetic sources for {@link DepositEvent} and {@link TransactionEvent}.
 */
public final class SyntheticSources {

    public final DataStream<DepositEvent> deposits;
    public final DataStream<TransactionEvent> transactions;

    /**
     * Creates and adds two synthetic sources for {@link DepositEvent} and {@link TransactionEvent}.
     *
     * @param env              the streaming environment to add the sources to.
     * @param recordsPerSecond the number of {@link TransactionEvent} per second to generate.
     * @return a {@link DataStream} for each event type generated.
     */
    public static SyntheticSources create(StreamExecutionEnvironment env, int recordsPerSecond) {

        final DataStreamSource<Either<DepositEvent, TransactionEvent>> depositsAndTransactions = env.addSource(
                new DepositsThenTransactionsSource(recordsPerSecond));

        final OutputTag<TransactionEvent> transactionsSideOutput = new OutputTag<>(
                "transactions side output",
                TypeInformation.of(TransactionEvent.class));

        final SingleOutputStreamOperator<DepositEvent> deposits = depositsAndTransactions.process(
                new ProcessFunction<Either<DepositEvent, TransactionEvent>, DepositEvent>() {

                    @Override
                    public void processElement(
                            Either<DepositEvent, TransactionEvent> depositOrTransaction,
                            Context context,
                            Collector<DepositEvent> out) {

                        if (depositOrTransaction.isLeft()) {
                            out.collect(depositOrTransaction.left());
                        }
                        else {
                            context.output(transactionsSideOutput, depositOrTransaction.right());
                        }
                    }
                });

        final DataStream<TransactionEvent> transactions = deposits.getSideOutput(transactionsSideOutput);

        return new SyntheticSources(deposits, transactions);
    }

    SyntheticSources(DataStream<DepositEvent> deposits, DataStream<TransactionEvent> transactions) {
        this.deposits = deposits;
        this.transactions = transactions;
    }
}

