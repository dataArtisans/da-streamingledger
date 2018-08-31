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

import com.dataartisans.streamingledger.sdk.api.StreamingLedger.State;
import com.dataartisans.streamingledger.sdk.api.TransactionProcessFunction.Context;

import java.util.function.Supplier;

/**
 * This interface provides access to values of a {@link State}.
 *
 * <p>The state access binds a specific {@link State} value and
 * a specific argument to a {@link TransactionProcessFunction} process function.
 *
 * <p>For example:<pre>
 * {@code
 * void process(InputEvent e, .., @State("age") StateAccess<Integer> age) { .. }
 * }
 * </pre>
 * Would bind {@code age} to a specific value (as identified by the input event {@code e}).
 * Calling {@link #read()} would return the bound value, and calling {@link #write(Object)} would
 * replace the value with the supplied argument.
 *
 * <p>Any change to this state access would made visible to other transactions, atomically as soon as the process
 * function completes, unless explicitly aborted via {@link Context#abort()}.
 *
 * @param <T> value parameter type.
 */
public interface StateAccess<T> {

    /**
     * Reads a value.
     *
     * @return the value bound to this state access.
     * @throws StateAccessException if this state access is not readable. i.e. it is not one of {@link AccessType#READ}
     *                              or {@link AccessType#READ_WRITE}.
     */
    T read() throws StateAccessException;

    /**
     * Reads a value by replaces missing values with a default supplier.
     *
     * @param defaultSupplier a default value supplier.
     * @return the value bound to this state access or default otherwise.
     * @throws StateAccessException if this state access is not readable. (only {@link AccessType#WRITE})
     */
    default T readOr(Supplier<T> defaultSupplier) throws StateAccessException {
        T read = read();
        if (read != null) {
            return read;
        }
        return defaultSupplier.get();
    }

    /**
     * Writes a value.
     *
     * @param newValue the value to bind with this state access.
     * @throws StateAccessException if this state access is not writeable. (only {@link AccessType#READ}).
     */
    void write(T newValue) throws StateAccessException;

    /**
     * Deletes a value.
     *
     * <p>Please note: that it would also delete the key associated with this value.
     *
     * @throws StateAccessException if this state access is not writeable. (only {@link AccessType#READ}).
     */
    void delete() throws StateAccessException;

    /**
     * @return the {@link State} name that this state access is referencing.
     */
    String getStateName();

    /**
     * @return the bind name of this state access.
     */
    String getStateAccessName();
}
