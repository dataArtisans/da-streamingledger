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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import com.dataartisans.streamingledger.sdk.common.union.TaggedElement;
import com.dataartisans.streamingledger.sdk.spi.StreamingLedgerSpec;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

final class SerialTransactor extends ProcessFunction<TaggedElement, Void> {

    private static final long serialVersionUID = 1;

    private final List<StreamingLedgerSpec<?, ?>> specs;
    private final List<OutputTag<Object>> sideOutputs;
    private final SideOutputContext<Object> collector;

    private transient SingleStreamSerialTransactor<Object, Object>[] transactors;

    SerialTransactor(List<StreamingLedgerSpec<?, ?>> specs, List<OutputTag<?>> sideOutputTags) {
        this.specs = requireNonNull(specs);
        this.sideOutputs = castOutputTags(sideOutputTags);
        this.collector = new SideOutputContext<>();
    }

    @SuppressWarnings("unchecked")
    private static SingleStreamSerialTransactor<Object, Object>[] newSingleStreamSerialTransactorArray(int n) {
        return (SingleStreamSerialTransactor<Object, Object>[]) new SingleStreamSerialTransactor[n];
    }

    @SuppressWarnings("unchecked")
    private static SingleStreamSerialTransactor<Object, Object> singleStreamSerialTransactorFromSpec(
            StreamingLedgerSpec<?, ?> aSpec,
            OutputTag<?> aTag,
            SideOutputContext<Object> collector,
            RuntimeContext runtimeContext) {

        OutputTag<Object> outputTag = (OutputTag<Object>) aTag;
        StreamingLedgerSpec<Object, Object> spec = (StreamingLedgerSpec<Object, Object>) aSpec;
        return new SingleStreamSerialTransactor<>(spec, outputTag, collector, runtimeContext);
    }

    @SuppressWarnings("unchecked")
    private static List<OutputTag<Object>> castOutputTags(List<OutputTag<?>> sideOutputTags) {
        return sideOutputTags.stream()
                .map(outputTag -> (OutputTag<Object>) outputTag)
                .collect(Collectors.toList());
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        final RuntimeContext runtimeContext = getRuntimeContext();

        SingleStreamSerialTransactor<Object, Object>[] transactors =
                newSingleStreamSerialTransactorArray(specs.size());

        // initialize the individual transactors
        for (int streamTag = 0; streamTag < specs.size(); streamTag++) {
            StreamingLedgerSpec<?, ?> aSpec = specs.get(streamTag);
            OutputTag<?> aTag = sideOutputs.get(streamTag);
            transactors[streamTag] = singleStreamSerialTransactorFromSpec(aSpec, aTag, collector, runtimeContext);
        }
        this.transactors = transactors;
    }

    @Override
    public void processElement(TaggedElement input, Context context, Collector<Void> unused) throws Exception {
        collector.setContext(context);

        final int streamTag = input.getDataStreamTag();
        SingleStreamSerialTransactor<Object, Object> transactor = transactors[streamTag];
        transactor.apply(input.getElement());
    }
}
