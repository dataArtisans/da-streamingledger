
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

package com.dataartisans.streamingledger.sdk.common.reflection;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.dataartisans.streamingledger.sdk.api.AccessType;
import com.dataartisans.streamingledger.sdk.api.StateAccess;
import com.dataartisans.streamingledger.sdk.api.StateAccessException;
import com.dataartisans.streamingledger.sdk.api.StreamingLedger.State;
import com.dataartisans.streamingledger.sdk.api.StreamingLedger.StateAccessSpec;
import com.dataartisans.streamingledger.sdk.api.TransactionProcessFunction;
import com.dataartisans.streamingledger.sdk.api.TransactionProcessFunction.Context;
import com.dataartisans.streamingledger.sdk.spi.StreamingLedgerSpec;
import com.dataartisans.streamingledger.sdk.spi.StreamingLedgerSpecFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test {@link ByteBuddyProcessFunctionInvoker}.
 */
public class ByteBuddyProcessFunctionInvokerTest {

    // --------------------------------------------------------------------------------------------------------------
    // Transaction Function.
    // --------------------------------------------------------------------------------------------------------------

    private ListContext<Long> context;

    // --------------------------------------------------------------------------------------------------------------
    // Test Setup.
    // --------------------------------------------------------------------------------------------------------------
    private StateAccessStub[] arguments;
    private StreamingLedgerSpec<String, Long> specification;

    private static StreamingLedgerSpec<String, Long> createSpecificationUnderTest() {
        State<Integer, Long> state = new State<>(
                "state",
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO);

        StateAccessSpec<String, Integer, Long> accessSpec = new StateAccessSpec<>(
                "value",
                state,
                String::hashCode,
                AccessType.READ_WRITE);

        List<StateAccessSpec<String, ?, ?>> bindings = Collections.singletonList(accessSpec);
        TypeInformation<String> inType = BasicTypeInfo.STRING_TYPE_INFO;
        TypeInformation<Long> outType = BasicTypeInfo.LONG_TYPE_INFO;
        return StreamingLedgerSpecFactory.create(new ReadingTransactionFunction(), bindings, inType, outType);
    }

    @Before
    public void before() {
        specification = createSpecificationUnderTest();
        context = new ListContext<>();
        arguments = new StateAccessStub[]{new StateAccessStub<Long>()};
    }

    // --------------------------------------------------------------------------------------------------------------
    // Tests.
    // --------------------------------------------------------------------------------------------------------------

    @Test
    public void usageExample() {
        ProcessFunctionInvoker<String, Long> generatedCodeThatInvokesUserCode =
                ByteBuddyProcessFunctionInvoker.create(specification);

        arguments[0].value = Long.MAX_VALUE;
        generatedCodeThatInvokesUserCode.invoke("does not matter", context, arguments);

        assertThat(context.emitted, hasItem(Long.MAX_VALUE));
    }

    // --------------------------------------------------------------------------------------------------------------
    // Test Utils.
    // --------------------------------------------------------------------------------------------------------------

    /**
     * ReadingTransactionFunction - simulates a user provided transaction function, that has a single state access.
     * This user code will read the provided value and emit it. See: {@link #process(String, Context, StateAccess)}.
     */
    private static class ReadingTransactionFunction extends TransactionProcessFunction<String, Long> {

        private static final long serialVersionUID = 1;

        @ProcessTransaction
        public void process(String input, Context<Long> out, @State("value") StateAccess<Long> value) {
            Long v = value.read();
            out.emit(v);
        }
    }

    private final class StateAccessStub<T> implements StateAccess<T> {
        T value;

        @Override
        public T read() throws StateAccessException {
            return value;
        }

        @Override
        public void write(T newValue) throws StateAccessException {
            value = newValue;
        }

        @Override
        public void delete() throws StateAccessException {
            value = null;
        }

        @Override
        public String getStateName() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getStateAccessName() {
            throw new UnsupportedOperationException();
        }
    }

    private final class ListContext<T> implements Context<T> {
        List<T> emitted = new ArrayList<>();

        @Override
        public void emit(T record) {
            emitted.add(record);
        }

        @Override
        public void abort() {
            emitted.clear();
        }
    }

}
