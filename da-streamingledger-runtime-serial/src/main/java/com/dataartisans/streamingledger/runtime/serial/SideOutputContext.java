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

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction.Context;
import org.apache.flink.util.OutputTag;

import com.dataartisans.streamingledger.sdk.api.TransactionProcessFunction;

import java.io.Serializable;
import java.util.ArrayList;

final class SideOutputContext<T> implements TransactionProcessFunction.Context<T>, Serializable {

    private static final long serialVersionUID = 1;
    private final ArrayList<T> records = new ArrayList<>();
    private transient OutputTag<T> outputTag;
    private transient ProcessFunction.Context context;
    private boolean aborted;

    void setContext(Context context) {
        this.context = context;
    }

    void setOutputTag(OutputTag<T> outputTag) {
        this.outputTag = outputTag;
    }

    void prepare() {
        aborted = false;
        records.clear();
    }

    @Override
    public void abort() {
        aborted = true;
        records.clear();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void emit(T record) {
        records.add(record);
    }

    boolean wasAborted() {
        return aborted;
    }

    void emitChanges() {
        if (aborted) {
            return;
        }
        for (T record : records) {
            context.output(outputTag, record);
        }
    }
}
