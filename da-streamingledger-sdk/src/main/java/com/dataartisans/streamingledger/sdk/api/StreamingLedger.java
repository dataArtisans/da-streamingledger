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

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.OutputTag;

import com.dataartisans.streamingledger.sdk.spi.InputAndSpec;
import com.dataartisans.streamingledger.sdk.spi.StreamingLedgerRuntimeLoader;
import com.dataartisans.streamingledger.sdk.spi.StreamingLedgerRuntimeProvider;
import com.dataartisans.streamingledger.sdk.spi.StreamingLedgerSpec;
import com.dataartisans.streamingledger.sdk.spi.StreamingLedgerSpecFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * StreamingLedger is a builder for a transactional operation on state involving multiple keys.
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public final class StreamingLedger {

    /**
     * a mapping between a stream name and a {@link InternalStreamBuilder}.
     */
    private final Map<String, InternalStreamBuilder<?, ?>> streamBuilders = new HashMap<>();

    /**
     * declared {@link State} names.
     */
    private final Set<String> stateNames = new HashSet<>();

    /**
     * the name of this streaming ledger.
     */
    private final String name;

    /**
     * Holds the translation result.
     */
    private ResultStreams resultStreams;

    /**
     * Private constructor, instantiation should go through {@link #create(String)}.
     */
    public StreamingLedger(String name) {
        this.name = requireNonNull(name);
    }

    // -------------------------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------------------------

    public static StreamingLedger create(String transactionalStreamsName) {
        return new StreamingLedger(transactionalStreamsName);
    }

    private static List<InputAndSpec<?, ?>> specsFromBuilders(Map<String, InternalStreamBuilder<?, ?>> builders) {
        List<InputAndSpec<?, ?>> inputAndSpecs = new ArrayList<>();

        builders.forEach((streamName, builder) -> {

            @SuppressWarnings({"unchecked", "raw"}) final InputAndSpec<?, ?> inputAndSpec = new InputAndSpec(
                    builder.getInputStream(),
                    streamName,
                    builder.buildTransactionalSpec()
            );

            inputAndSpecs.add(inputAndSpec);
        });

        return inputAndSpecs;
    }

    /**
     * Creates a {@link StateBuilder} for {@link State} with a given name.
     *
     * @param stateName a name to associate with this state.
     */
    public StateBuilder declareState(String stateName) {
        requireNonNull(stateName);
        checkNotTranslated();

        if (!stateNames.add(stateName)) {
            throw new IllegalArgumentException("A state named '" + stateName + "' is already defined.");
        }
        return new StateBuilder(stateName);
    }

    /**
     * Creates a new streaming ledger operation for the events of the given data stream.
`     * of a streaming ledger state operation.
     *
     * @param inputStream The data stream with the transaction data events.
     * @param name        The unique name for this operation, used for state matching during upgrades.
     * @param <InT>       The type of the transaction data events.
     * @return An instance of the builder for the
     */
    public <InT> StreamBuilder<InT> usingStream(DataStream<InT> inputStream, String name) {
        requireNonNull(inputStream);
        requireNonNull(name);
        checkNotTranslated();

        InternalStreamBuilder<InT, ?> builder = new InternalStreamBuilder<>(inputStream, name);
        if (streamBuilders.containsKey(name)) {
            throw new IllegalArgumentException("stream named '" + name + "' already exists.");
        }
        streamBuilders.put(name, builder);
        return new StreamBuilder<>(builder);
    }

    /**
     * Constructs a {@link ResultStreams} as defined.
     */
    public ResultStreams resultStreams() {
        if (resultStreams == null) {
            List<InputAndSpec<?, ?>> specs = specsFromBuilders(streamBuilders);
            StreamingLedgerRuntimeProvider runtimeProvider = StreamingLedgerRuntimeLoader.getRuntimeProvider();
            resultStreams = runtimeProvider.translate(name, specs);
        }
        return resultStreams;
    }

    // -------------------------------------------------------------------------------------------
    // Nested Classes
    // -------------------------------------------------------------------------------------------

    private void checkNotTranslated() {
        if (resultStreams != null) {
            throw new IllegalStateException("Can't be called after calling getResultStream().");
        }
    }

    /**
     * A specification of a particular state access that happens as part of a transaction.
     *
     * @param <EventT> The type of the transaction data event.
     * @param <K>      The type of the key in this state access.
     * @param <V>      The type of the value stored in the state.
     */
    public static class StateAccessSpec<EventT, K, V> implements Serializable {

        private static final long serialVersionUID = 1;

        /**
         * The name under which this specific state access is passed to the {@link TransactionProcessFunction}'s
         * process method.
         */
        public final String bindName;

        /**
         * The streaming ledger state to access.
         */
        public final State<K, V> state;

        /**
         * The function that gets the key for this state access from the transaction data event.
         */
        public final KeySelector<EventT, K> keyAccess;

        /**
         * The access type, whether state is only read, only written, or both.
         */
        public final AccessType accessType;

        /**
         * Creates a new StateAccessSpec.
         *
         * @param bindName   The name under which this specific state access is passed to the
         *                   {@link TransactionProcessFunction}'s process method.
         * @param state      The streaming ledger state to access.
         * @param keyAccess  The function that gets the key for this state access from the
         *                   transaction data event.
         * @param accessType The access type, whether state is only read, only written, or both.
         */
        public StateAccessSpec(
                String bindName,
                State<K, V> state,
                KeySelector<EventT, K> keyAccess,
                AccessType accessType) {
            this.bindName = requireNonNull(bindName);
            this.state = requireNonNull(state);
            this.keyAccess = requireNonNull(keyAccess);
            this.accessType = requireNonNull(accessType);
        }
    }

    /**
     * An abstract description of a streaming ledger transactional state maintained by Flink
     * in a transactional scope (as built via {@link StreamingLedger}.
     *
     * @param <K> The type of the key that accesses the state.
     * @param <V> The type of the value stored in the state.
     */
    public static final class State<K, V> implements Serializable {

        private static final long serialVersionUID = 1;

        private final String name;
        private final TypeInformation<K> keyType;
        private final TypeInformation<V> valueType;

        public State(String name, TypeInformation<K> keyType, TypeInformation<V> valueType) {
            this.name = requireNonNull(name);
            this.keyType = requireNonNull(keyType);
            this.valueType = requireNonNull(valueType);
        }

        // ------------------------------------------------------------------------
        //  Properties
        // ------------------------------------------------------------------------

        public String getName() {
            return name;
        }

        public TypeInformation<K> getKeyType() {
            return keyType;
        }

        public TypeInformation<V> getValueType() {
            return valueType;
        }

        @Override
        public String toString() {
            return "State (" + name + ')';
        }
    }

    // -------------------------------------------------------------------------------------------
    // State Builder
    // -------------------------------------------------------------------------------------------

    /**
     * An {@code ResultStreams} of a transactional stream.
     */
    public static final class ResultStreams {

        private final Map<String, DataStream<?>> resultStreams;

        public ResultStreams(Map<String, DataStream<?>> resultStreams) {
            this.resultStreams = resultStreams;
        }

        public Iterable<? extends DataStream<?>> getResultStreams() {
            return resultStreams.values();
        }

        public <OutT> DataStream<OutT> getResultStream(OutputTag<OutT> output) {
            requireNonNull(output);
            @SuppressWarnings("unchecked") DataStream<OutT> stream = (DataStream<OutT>)
                    resultStreams.get(output.getId());
            if (stream == null) {
                throw new IllegalArgumentException("unknown stream named '" + output.getId() + "'.");
            }
            return stream;
        }
    }

    /**
     * A {@link State} builder.
     */
    public static final class StateBuilder {
        private final String name;

        StateBuilder(String name) {
            this.name = requireNonNull(name);
        }

        public <K> StateBuilderWithKeyType<K> withKeyType(Class<K> keyType) {
            return withKeyType(TypeInformation.of(keyType));
        }

        public <K> StateBuilderWithKeyType<K> withKeyType(TypeHint<K> keyType) {
            return withKeyType(TypeInformation.of(keyType));
        }

        public <K> StateBuilderWithKeyType<K> withKeyType(TypeInformation<K> keyType) {
            return new StateBuilderWithKeyType<>(name, keyType);
        }
    }

    // -------------------------------------------------------------------------------------------
    // Stream Builder
    // -------------------------------------------------------------------------------------------

    /**
     * A {@link State} builder with a {@code name} and {@code keyType}.
     *
     * @param <K> key type
     */
    public static final class StateBuilderWithKeyType<K> {
        private final String name;
        private final TypeInformation<K> keyType;

        StateBuilderWithKeyType(String name, TypeInformation<K> keyType) {
            this.name = requireNonNull(name);
            this.keyType = requireNonNull(keyType);
        }

        public <V> State<K, V> withValueType(Class<V> valueType) {
            return new State<>(name, keyType, TypeInformation.of(valueType));
        }

        public <V> State<K, V> valueType(TypeHint<V> valueType) {
            return new State<>(name, keyType, TypeInformation.of(valueType));
        }

        public <V> State<K, V> valueType(TypeInformation<V> valueType) {
            return new State<>(name, keyType, valueType);
        }
    }

    /**
     * Internal builder that is created per input stream, and holds the necessary information gathered
     * at different phase of {@link StreamingLedger#usingStream(DataStream, String)}.
     */
    @SuppressWarnings("UnusedReturnValue")
    private static final class InternalStreamBuilder<InT, ResultT> {

        /**
         * All state accesses (read, write, read-write) that the transaction processing requires.
         */
        private final List<StateAccessSpec<InT, ?, ?>> accesses;

        /**
         * The input stream with the transaction data events.
         */
        private final DataStream<InT> inputStream;

        /**
         * The name associated with this stream.
         */
        private final String name;

        /**
         * Would be set by {@link #withProcessFunction(TransactionProcessFunction)}.
         */
        @Nullable
        private TransactionProcessFunction<InT, ResultT> processFunction;

        /**
         * Would be deduced by {@link #withProcessFunction(TransactionProcessFunction)}.
         */
        @Nullable
        private TypeInformation<ResultT> resultType;

        InternalStreamBuilder(DataStream<InT> inputStream, String name) {
            this.inputStream = requireNonNull(inputStream);
            this.accesses = new ArrayList<>();
            this.name = requireNonNull(name);
        }

        /**
         * deduce the result type of the supplied {@link TransactionProcessFunction}.
         */
        private static <InT, ResultT> TypeInformation<ResultT> deduceResultType(
                TransactionProcessFunction<InT, ResultT> processor,
                TypeInformation<InT> inputType) {

            if (processor instanceof ResultTypeQueryable) {
                @SuppressWarnings("unchecked")
                TypeInformation<ResultT> producedType = ((ResultTypeQueryable<ResultT>) processor).getProducedType();
                return producedType;
            }
            return TypeExtractor.createTypeInfo(
                    TransactionProcessFunction.class,
                    processor.getClass(),
                    1,
                    inputType,
                    null);
        }

        InternalStreamBuilder<InT, ResultT> withProcessFunction(TransactionProcessFunction<InT, ResultT> processor) {
            this.processFunction = requireNonNull(processor);
            this.resultType = deduceResultType(processor, inputStream.getType());
            return this;
        }

        InternalStreamBuilder<InT, ResultT> withAccess(StateAccessSpec<InT, ?, ?> spec) {
            accesses.add(spec);
            return this;
        }

        StreamingLedgerSpec<InT, ResultT> buildTransactionalSpec() {
            if (processFunction == null || resultType == null) {
                throw new IllegalStateException("A TransactionProcessFunction is missing.");
            }
            return StreamingLedgerSpecFactory.create(processFunction, accesses, inputStream.getType(), resultType);
        }

        DataStream<InT> getInputStream() {
            return inputStream;
        }
    }

    /**
     * A fluent builder that is used to define all the aspects of a transactional stream.
     *
     * @param <InT> input stream data type
     */
    public static final class StreamBuilder<InT> {
        private final InternalStreamBuilder<InT, ?> builder;

        StreamBuilder(InternalStreamBuilder<InT, ?> streamBuilder) {
            this.builder = streamBuilder;
        }

        public <OutT> StreamBuilderWithProcessFunction<InT, OutT> apply(TransactionProcessFunction<InT, OutT> fn) {
            @SuppressWarnings("unchecked")
            InternalStreamBuilder<InT, OutT> b = (InternalStreamBuilder<InT, OutT>) builder;
            return new StreamBuilderWithProcessFunction<>(b.withProcessFunction(fn));
        }
    }

    /**
     * A fluent builder that is used to define all the aspects of a transactional stream.
     *
     * @param <InT>  input stream data type
     * @param <OutT> output stream data type
     */
    public static final class StreamBuilderWithProcessFunction<InT, OutT> {
        private final InternalStreamBuilder<InT, OutT> builder;

        StreamBuilderWithProcessFunction(InternalStreamBuilder<InT, OutT> builder) {
            this.builder = builder;
        }

        /**
         * Adds another state access to the transactional operation. A state access is needed for
         * each "row" or "entry" in the state that the transactional operation wants to modify.
         *
         * <p>For example, if the transactional operation modifies two entries in a streaming ledger state,
         * two state accesses must be specified.
         *
         * @param bindName   The name under which this specific state access is passed to the
         *                   {@link TransactionProcessFunction}'s process method.
         * @param state      The streaming ledger state to access.
         * @param keyAccess  The function that gets the key for this state access from the
         *                   transaction data event.
         * @param accessType The access type, whether state is only read, only written, or both.
         * @return This instance, to allow function chaining.
         */
        public <K, V> StreamBuilderWithProcessFunction<InT, OutT> on(
                State<K, V> state,
                KeySelector<InT, K> keyAccess,
                String bindName,
                AccessType accessType) {
            builder.withAccess(new StateAccessSpec<>(bindName, state, keyAccess, accessType));
            return this;
        }

        public OutputTag<OutT> output() {
            TypeInformation<OutT> outType = requireNonNull(builder.resultType);
            return new OutputTag<>(builder.name, outType);
        }
    }
}
