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

package com.dataartisans.streamingledger.sdk.common.union;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Union differently typed {@link DataStream}s.
 */
public final class Union {

    private Union() {
    }

    /**
     * Union differently typed {@link DataStream}s into single {@code DataStream}.
     *
     * <p>The resulting {@code DataStream} is of type {@link TaggedElement} where
     * {@link TaggedElement#getDataStreamTag()} corresponds to the list position of the source {@code DataStream} in
     * {@code inputs} that produced that element, and {@link TaggedElement#getElement()} is the element produced.
     *
     * @param inputs the input data streams to union.
     * @return a {@code DataStream} that corresponds to the union of all the input {@link DataStream}s
     */
    public static DataStream<TaggedElement> apply(List<DataStream<?>> inputs) {
        checkArgument(!inputs.isEmpty(), "union requires at least one input data stream.");

        List<DataStream<TaggedElement>> taggedInputs = tagInputStreams(inputs);
        if (taggedInputs.size() == 1) {
            return taggedInputs.get(0);
        }
        DataStream<TaggedElement> first = taggedInputs.get(0);
        List<DataStream<TaggedElement>> restList = taggedInputs.subList(1, taggedInputs.size());

        @SuppressWarnings({"unchecked", "raw"})
        DataStream<TaggedElement>[] restArray = (DataStream<TaggedElement>[]) new DataStream[restList.size()];
        DataStream<TaggedElement>[] rest = restList.toArray(restArray);
        return first.union(rest);
    }

    // -------------------------------------------------------------------------------------------------------
    // Internal Helpers
    // -------------------------------------------------------------------------------------------------------

    private static List<DataStream<TaggedElement>> tagInputStreams(List<DataStream<?>> inputs) {
        TypeInformation<TaggedElement> typeInfo = createUnionTypeInfo(inputs);

        List<DataStream<TaggedElement>> taggedInputs = new ArrayList<>();
        int dataStreamIndex = 0;
        for (DataStream<?> input : inputs) {

            final DataStream<TaggedElement> transformed = input
                    .map(new TaggingMap<>(dataStreamIndex))
                    .returns(typeInfo);

            dataStreamIndex++;
            taggedInputs.add(transformed);
        }
        return taggedInputs;
    }

    private static UnionTypeInfo createUnionTypeInfo(List<DataStream<?>> inputs) {
        List<TypeInformation<?>> underlyingTypes = inputs.stream()
                .map(DataStream::getType)
                .collect(Collectors.toList());

        return new UnionTypeInfo(underlyingTypes);
    }

    // -------------------------------------------------------------------------------------------------------
    // Nested Class
    // -------------------------------------------------------------------------------------------------------

    private static final class TaggingMap<InT> extends RichMapFunction<InT, TaggedElement> {

        private static final long serialVersionUID = 1;

        private final int dataStreamIndex;
        private transient TaggedElement union;

        TaggingMap(int dataStreamIndex) {
            this.dataStreamIndex = dataStreamIndex;
        }

        @Override
        public TaggedElement map(InT element) {
            union.setElement(element);
            return union;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            this.union = new TaggedElement(this.dataStreamIndex, null);
        }
    }
}
