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

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.java.functions.KeySelector;

import com.dataartisans.streamingledger.sdk.api.AccessType;
import com.dataartisans.streamingledger.sdk.api.StateAccess;
import com.dataartisans.streamingledger.sdk.api.StateAccessException;
import com.dataartisans.streamingledger.sdk.api.StateNotReadableException;
import com.dataartisans.streamingledger.sdk.api.StateNotWritableException;
import com.dataartisans.streamingledger.sdk.api.StreamingLedger.StateAccessSpec;

import static java.util.Objects.requireNonNull;

final class SerialStateAccess<InT, K, V> implements StateAccess<V> {
    private final StateAccessSpec<InT, K, V> spec;
    private final MapState<K, V> state;

    private final KeySelector<InT, K> keySelector;
    private final boolean writeOnly;
    private final boolean readOnly;

    private K key;
    private V value;
    private boolean changed;

    SerialStateAccess(StateAccessSpec<InT, K, V> spec, MapState<K, V> state) {
        this.spec = requireNonNull(spec);
        this.state = requireNonNull(state);
        this.keySelector = requireNonNull(spec.keyAccess);
        this.writeOnly = spec.accessType == AccessType.WRITE;
        this.readOnly = spec.accessType == AccessType.READ;
    }

    @Override
    public V read() throws StateAccessException {
        if (writeOnly) {
            throw new StateNotReadableException(this);
        }
        return value;
    }

    @Override
    public void write(V newValue) throws StateAccessException {
        if (readOnly) {
            throw new StateNotWritableException(this);
        }
        this.value = newValue;
        this.changed = true;
    }

    @Override
    public void delete() throws StateAccessException {
        if (readOnly) {
            throw new StateNotWritableException(this);
        }
        this.value = null;
        this.changed = true;
    }

    @Override
    public String getStateName() {
        return spec.state.getName();
    }

    @Override
    public String getStateAccessName() {
        return spec.bindName;
    }

    // -----------------------------------------------------
    // For internal use by SerialTransactor
    // -----------------------------------------------------

    void prepare(InT input) throws Exception {
        final K key = keySelector.getKey(input);
        this.key = key;
        this.changed = false;
        if (!writeOnly) {
            this.value = state.get(key);
        }
    }

    void commit(boolean wasAborted) throws Exception {
        if (!changed || wasAborted) {
            return;
        }
        if (value == null) {
            state.remove(key);
        }
        else {
            state.put(key, value);
        }
    }
}
