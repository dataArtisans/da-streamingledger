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

package com.dataartisans.streamingledger.runtime.serial;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.util.OutputTag;

import com.dataartisans.streamingledger.sdk.api.StreamingLedger.StateAccessSpec;
import com.dataartisans.streamingledger.sdk.common.reflection.ByteBuddyProcessFunctionInvoker;
import com.dataartisans.streamingledger.sdk.common.reflection.ProcessFunctionInvoker;
import com.dataartisans.streamingledger.sdk.spi.StreamingLedgerSpec;

import static java.util.Objects.requireNonNull;

/**
 * A serially executing transactor.
 *
 * @param <InT>     transaction event type
 * @param <ResultT> transaction result type
 */
final class SingleStreamSerialTransactor<InT, ResultT> {
    private final ProcessFunctionInvoker<InT, ResultT> userFunction;
    private final SerialStateAccess<InT, ?, ?>[] accesses;
    private final OutputTag<ResultT> sideOutputTag;
    private final SideOutputContext<ResultT> context;

    SingleStreamSerialTransactor(
            StreamingLedgerSpec<InT, ResultT> spec,
            OutputTag<ResultT> sideOutputTag,
            SideOutputContext<ResultT> context,
            RuntimeContext runtimeContext) {

        requireNonNull(spec);
        requireNonNull(sideOutputTag);
        requireNonNull(runtimeContext);

        this.context = context;
        this.sideOutputTag = sideOutputTag;
        this.accesses = createStateAccessesFromSpec(spec, runtimeContext);
        this.userFunction = ByteBuddyProcessFunctionInvoker.create(spec);
    }

    @SuppressWarnings("unchecked")
    private static <T> SerialStateAccess<T, ?, ?>[] newStateAccessArray(int n) {
        return (SerialStateAccess<T, ?, ?>[]) new SerialStateAccess[n];
    }

    // --------------------------------------------------------------------------------------------------------
    // Internal helpers
    // --------------------------------------------------------------------------------------------------------

    private static <K, V> MapState<K, V> fromSpec(StateAccessSpec<?, K, V> spec, RuntimeContext context) {
        MapStateDescriptor<K, V> descriptor = new MapStateDescriptor<>(
                spec.state.getName(),
                spec.state.getKeyType(),
                spec.state.getValueType());

        return context.getMapState(descriptor);
    }

    void apply(InT input) throws Exception {
        context.setOutputTag(sideOutputTag);
        //
        // prepare
        //
        for (SerialStateAccess<InT, ?, ?> access : accesses) {
            access.prepare(input);
        }
        context.prepare();
        //
        // invoke
        //
        userFunction.invoke(input, context, accesses);
        //
        // commit or abort
        //
        for (SerialStateAccess<InT, ?, ?> access : accesses) {
            access.commit(context.wasAborted());
        }
        context.emitChanges();
    }

    private SerialStateAccess<InT, ?, ?>[] createStateAccessesFromSpec(
            StreamingLedgerSpec<InT, ResultT> spec,
            RuntimeContext ctx) {

        SerialStateAccess<InT, ?, ?>[] accesses = newStateAccessArray(spec.stateBindings.size());

        for (int i = 0; i < accesses.length; i++) {
            final StateAccessSpec<InT, ?, ?> accessSpec = spec.stateBindings.get(i);

            MapState<?, ?> state = fromSpec(accessSpec, ctx);

            @SuppressWarnings({"rawtypes", "unchecked"})
            SerialStateAccess<InT, ?, ?> serialStateAccess = new SerialStateAccess(accessSpec, state);
            accesses[i] = serialStateAccess;
        }
        return accesses;
    }
}
