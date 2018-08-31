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

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

import com.dataartisans.streamingledger.sdk.api.StreamingLedger.ResultStreams;
import com.dataartisans.streamingledger.sdk.common.union.TaggedElement;
import com.dataartisans.streamingledger.sdk.common.union.Union;
import com.dataartisans.streamingledger.sdk.spi.InputAndSpec;
import com.dataartisans.streamingledger.sdk.spi.StreamingLedgerRuntimeProvider;
import com.dataartisans.streamingledger.sdk.spi.StreamingLedgerSpec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A {@link StreamingLedgerRuntimeProvider} for the default serial transaction implementation.
 */
public final class SerialStreamingLedgerRuntimeProvider implements StreamingLedgerRuntimeProvider {

    private static DataStream<TaggedElement> union(List<InputAndSpec<?, ?>> inputAndSpecs) {
        List<DataStream<?>> inputs = inputAndSpecs.stream()
                .map(inputAndSpec -> inputAndSpec.inputStream)
                .collect(Collectors.toList());

        return Union.apply(inputs);
    }

    // -------------------------------------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------------------------------------

    private static List<OutputTag<?>> createSideOutputTags(List<InputAndSpec<?, ?>> specs) {
        List<OutputTag<?>> outputTags = new ArrayList<>();
        for (InputAndSpec<?, ?> streamWithSpec : specs) {
            OutputTag<?> outputTag = new OutputTag<>(streamWithSpec.streamName, streamWithSpec.streamSpec.resultType);
            outputTags.add(outputTag);
        }
        return outputTags;
    }

    private static List<StreamingLedgerSpec<?, ?>> specs(List<InputAndSpec<?, ?>> inputAndSpecs) {
        return inputAndSpecs.stream()
                .map(inputAndSpec -> inputAndSpec.streamSpec)
                .collect(Collectors.toList());
    }

    @Override
    public ResultStreams translate(String name, List<InputAndSpec<?, ?>> streamLedgerSpecs) {
        List<OutputTag<?>> sideOutputTags = createSideOutputTags(streamLedgerSpecs);

        // the input stream is a union of different streams.
        KeyedStream<TaggedElement, Boolean> input = union(streamLedgerSpecs)
                .keyBy(unused -> true);

        // main pipeline
        String serialTransactorName = "SerialTransactor(" + name + ")";
        SingleOutputStreamOperator<Void> resultStream = input
                .process(new SerialTransactor(specs(streamLedgerSpecs), sideOutputTags))
                .name(serialTransactorName)
                .uid(serialTransactorName + "___SERIAL_TX")
                .forceNonParallel()
                .returns(Void.class);

        // gather the sideOutputs.
        Map<String, DataStream<?>> output = new HashMap<>();
        for (OutputTag<?> outputTag : sideOutputTags) {
            DataStream<?> rs = resultStream.getSideOutput(outputTag);
            output.put(outputTag.getId(), rs);
        }
        return new ResultStreams(output);
    }
}
