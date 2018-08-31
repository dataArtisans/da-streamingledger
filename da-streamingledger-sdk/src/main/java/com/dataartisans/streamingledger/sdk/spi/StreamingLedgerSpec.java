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

import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.dataartisans.streamingledger.sdk.api.StreamingLedger;
import com.dataartisans.streamingledger.sdk.api.StreamingLedger.StateAccessSpec;
import com.dataartisans.streamingledger.sdk.api.TransactionProcessFunction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * A {@code StreamingLedgerSpec} contains all the necessary information gathered at
 * {@link StreamingLedger#usingStream} to execute the process function at runtime.
 *
 * @param <InT>     The type of the transaction data events.
 * @param <ResultT> The type of the transaction data results.
 */
public final class StreamingLedgerSpec<InT, ResultT> implements Serializable {

    private static final long serialVersionUID = 1;

    // ------------------------------------------------------------------------
    //  Properties
    // ------------------------------------------------------------------------

    public final TransactionProcessFunction<InT, ResultT> processFunction;
    public final String processMethodName;
    public final List<StateAccessSpec<InT, ?, ?>> stateBindings;
    public final TypeInformation<InT> inputType;
    public final TypeInformation<ResultT> resultType;

    StreamingLedgerSpec(
            TransactionProcessFunction<InT, ResultT> processFunction,
            String processMethodName,
            List<StateAccessSpec<InT, ?, ?>> stateBindings,
            TypeInformation<InT> inputType,
            TypeInformation<ResultT> resultType) {
        this.processFunction = requireNonNull(processFunction);
        this.processMethodName = requireNonNull(processMethodName);
        this.stateBindings = Collections.unmodifiableList(new ArrayList<>(stateBindings));
        this.inputType = requireNonNull(inputType);
        this.resultType = requireNonNull(resultType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StreamingLedgerSpec<?, ?> that = (StreamingLedgerSpec<?, ?>) o;
        return Objects.equals(processFunction, that.processFunction)
                && Objects.equals(processMethodName, that.processMethodName)
                && Objects.equals(stateBindings, that.stateBindings)
                && Objects.equals(inputType, that.inputType)
                && Objects.equals(resultType, that.resultType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(processFunction, processMethodName, stateBindings, inputType, resultType);
    }
}
