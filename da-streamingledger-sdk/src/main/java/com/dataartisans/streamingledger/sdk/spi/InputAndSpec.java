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

package com.dataartisans.streamingledger.sdk.spi;

import org.apache.flink.streaming.api.datastream.DataStream;

import com.dataartisans.streamingledger.sdk.api.StreamingLedger;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * An input to {@link StreamingLedgerRuntimeProvider#translate(String, List)}.
 * This would be created by {@link StreamingLedger#resultStreams()}.
 */
public final class InputAndSpec<InT, ResultT> {

    public final DataStream<InT> inputStream;

    public final String streamName;

    public final StreamingLedgerSpec<InT, ResultT> streamSpec;

    public InputAndSpec(DataStream<InT> inputStream, String streamName, StreamingLedgerSpec<InT, ResultT> spec) {
        this.inputStream = requireNonNull(inputStream);
        this.streamName = requireNonNull(streamName);
        this.streamSpec = requireNonNull(spec);
    }
}
