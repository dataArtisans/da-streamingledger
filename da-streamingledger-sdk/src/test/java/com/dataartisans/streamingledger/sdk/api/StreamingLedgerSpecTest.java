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

package com.dataartisans.streamingledger.sdk.api;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.dataartisans.streamingledger.sdk.api.StreamingLedger.State;
import com.dataartisans.streamingledger.sdk.api.StreamingLedger.StateAccessSpec;
import com.dataartisans.streamingledger.sdk.spi.StreamingLedgerSpec;
import com.dataartisans.streamingledger.sdk.spi.StreamingLedgerSpecFactory;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test {@link StreamingLedgerSpec}.
 */
public class StreamingLedgerSpecTest {

    private static <InT, OutT> StreamingLedgerSpec<InT, OutT> createSpecificationUnderTest(
            TransactionProcessFunction<InT, OutT> processFunction,
            TypeInformation<InT> inType,
            TypeInformation<OutT> outType) {

        State<Integer, Long> state = new State<>(
                "state",
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO);

        StateAccessSpec<InT, Integer, Long> accessSpec = new StateAccessSpec<>(
                "value",
                state,
                Object::hashCode,
                AccessType.READ_WRITE);

        List<StateAccessSpec<InT, ?, ?>> bindings = Collections.singletonList(accessSpec);
        return StreamingLedgerSpecFactory.create(processFunction, bindings, inType, outType);
    }

    @Test
    public void example() {

        final class TxnFn extends TransactionProcessFunction<String, Long> {

            private static final long serialVersionUID = 1;

            @ProcessTransaction
            public void process(String input, Context<Long> out, @State("value") StateAccess<Long> value) {
                Long v = value.read();
                out.emit(v);
            }
        }

        StreamingLedgerSpec<String, Long> spec = createSpecificationUnderTest(
                new TxnFn(),
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO);

        assertThat(spec.processMethodName, is("process"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void missingProcessTransactionAnnotation() {

        final class MissingProcessTxnAnnotation extends TransactionProcessFunction<String, Long> {

            private static final long serialVersionUID = 1;

            // Ooops: @ProcessTransaction
            public void process(Boolean input, Context<Long> out, @State("value") StateAccess<Long> value) {
                Long v = value.read();
                out.emit(v);
            }
        }

        createSpecificationUnderTest(
                new MissingProcessTxnAnnotation(),
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO);
    }

    @Test(expected = IllegalArgumentException.class)
    public void twoFunctionsAnnotated() {

        final class TwoFunctionsAnnotated extends TransactionProcessFunction<String, Long> {

            private static final long serialVersionUID = 1;

            @ProcessTransaction
            public void process1(Boolean input, Context<Long> out, @State("value") StateAccess<Long> value) {
                Long v = value.read();
                out.emit(v);
            }

            @ProcessTransaction
            public void process2(Boolean input, Context<Long> out, @State("value") StateAccess<Long> value) {
                Long v = value.read();
                out.emit(v);
            }
        }

        createSpecificationUnderTest(
                new TwoFunctionsAnnotated(),
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO);
    }

    @Test
    public void subClassingWorks() {

        class ParentClass extends TransactionProcessFunction<String, Long> {

            private static final long serialVersionUID = 1;

            @ProcessTransaction
            public void process(String input, Context<Long> out, @State("value") StateAccess<Long> value) {
                Long v = value.read();
                out.emit(v);
            }

        }

        final class SubClass extends ParentClass {
        }

        createSpecificationUnderTest(
                new SubClass(),
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO);
    }

    @Test(expected = IllegalArgumentException.class)
    public void wrongInputParameter() {

        final class WrongInputParameter extends TransactionProcessFunction<String, Long> {

            private static final long serialVersionUID = 1;

            @ProcessTransaction
            public void process(Boolean input, Context<Long> out, @State("value") StateAccess<Long> value) {
                Long v = value.read();
                out.emit(v);
            }
        }

        createSpecificationUnderTest(
                new WrongInputParameter(),
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO);
    }

    @Test(expected = IllegalArgumentException.class)
    public void wrongContextTypeParameter() {

        final class WrongContextParameter extends TransactionProcessFunction<String, Long> {

            private static final long serialVersionUID = 1;

            @ProcessTransaction
            public void process(String input, Context<Boolean> out, @State("value") StateAccess<Long> value) {
                out.emit(true);
            }
        }

        createSpecificationUnderTest(
                new WrongContextParameter(),
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO);
    }

    @Test(expected = IllegalArgumentException.class)
    public void unknownStateBindingName() {

        final class UnknownStateBindingName extends TransactionProcessFunction<String, Long> {

            private static final long serialVersionUID = 1;

            @ProcessTransaction
            public void process(String input, Context<Long> out, @State("xyz") StateAccess<Long> value) {
                out.emit(1L);
            }
        }

        createSpecificationUnderTest(
                new UnknownStateBindingName(),
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO);
    }

    @Test(expected = IllegalArgumentException.class)
    public void wrongStateAccessTypeParameter() {

        final class UnknownStateBindingName extends TransactionProcessFunction<String, Long> {

            private static final long serialVersionUID = 1;

            @ProcessTransaction
            public void process(String input, Context<Long> out, @State("value") StateAccess<Boolean> value) {
                out.emit(1L);
            }
        }

        createSpecificationUnderTest(
                new UnknownStateBindingName(),
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO);
    }

    @Test(expected = IllegalArgumentException.class)
    public void stateBindingIsNotAnnotatedWithStateAnnotation() {

        final class MissingStateAnnotation extends TransactionProcessFunction<String, Long> {

            private static final long serialVersionUID = 1;

            @ProcessTransaction
            public void process(String input, Context<Long> out, StateAccess<Long> value) {
                out.emit(1L);
            }
        }

        createSpecificationUnderTest(
                new MissingStateAnnotation(),
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO);
    }

    @Test(expected = IllegalArgumentException.class)
    public void missingStateAccess() {

        final class MissingStateAccess extends TransactionProcessFunction<String, Long> {

            private static final long serialVersionUID = 1;

            @ProcessTransaction
            public void process(String input, Context<Long> out, @State("value") Long value) {
                out.emit(1L);
            }
        }

        createSpecificationUnderTest(
                new MissingStateAccess(),
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO);
    }

}
