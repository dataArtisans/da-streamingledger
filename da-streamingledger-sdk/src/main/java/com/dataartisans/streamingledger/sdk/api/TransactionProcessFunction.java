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

import org.apache.flink.api.common.functions.Function;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * TransactionProcessFunction.
 *
 * @param <InT>  The type of the transaction input events.
 * @param <OutT> The type of the transaction result events.
 */
public abstract class TransactionProcessFunction<InT, OutT> implements Function {

    // stable serialVersionUID for compatibility across code updates
    private static final long serialVersionUID = 1L;

    // ------------------------------------------------------------------------

    /**
     * This interface marks the so called {@code 'process method'}, which processes the
     * transaction with the transaction event and the involved states.
     *
     * <p>This interface is declared within the TransactionProcessFunction class so that
     * users do not need to import anything when writing their process method.
     */
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    public @interface ProcessTransaction {
    }

    // ------------------------------------------------------------------------

    /**
     * Annotation for state access parameters in the process method.
     *
     * <p>This interface is declared within the TransactionProcessFunction class so that
     * users do not need to import anything when writing their process method.
     */
    @Target(ElementType.PARAMETER)
    @Retention(RetentionPolicy.RUNTIME)
    public @interface State {

        /**
         * The value is the name of the state access.
         */
        String value();

    }

    // ------------------------------------------------------------------------

    /**
     * Transaction Processing Context.
     */
    public interface Context<T> {

        /**
         * Emits a transaction result.
         */
        void emit(T record);

        /**
         * Abort the current transaction.
         *
         * <p>Calling abort will undo any change made to a {@link StateAccess}, and will not cause a transaction result
         * to be emitted.
         */
        void abort();
    }
}
