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

import com.dataartisans.streamingledger.sdk.api.StreamingLedger.ResultStreams;

import java.util.List;

/**
 * The stream ledger runtime provider is responsible for taking a stream ledger
 * program and creating the Flink streaming graph that should execute it.
 *
 * <p>The exact way of executing the distributed transactions is implementation specific.
 * Some runtime providers instantiate a serial (trivially correct) execution model, some
 * a sophisticated parallel data flow.
 */
public interface StreamingLedgerRuntimeProvider {

    ResultStreams translate(String name, List<InputAndSpec<?, ?>> streamLedgerSpecs);
}
