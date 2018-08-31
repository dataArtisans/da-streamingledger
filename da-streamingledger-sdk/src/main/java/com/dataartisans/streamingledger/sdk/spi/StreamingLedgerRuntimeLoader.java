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

import org.apache.flink.util.FlinkRuntimeException;

import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

/**
 * A loader utility to find and load the {@link StreamingLedgerRuntimeProvider} to
 * execute the program with.
 */
public class StreamingLedgerRuntimeLoader {

    /**
     * The lock to make sure only one transaction runtime provider is ever loaded.
     */
    private static final ReentrantLock LOCK = new ReentrantLock();

    /**
     * The transaction runtime provider, null if not yet loaded.
     */
    @Nullable
    private static StreamingLedgerRuntimeProvider runtimeProvider;

    /**
     * Gets the runtime provider to execute the transactions. This method will load
     * the runtime provider if it has not been loaded before.
     *
     * <p>If more than one runtime provider is found, this method throws an
     * exception, because it cannot determine which provider to use.
     */
    public static StreamingLedgerRuntimeProvider getRuntimeProvider() {
        LOCK.lock();
        try {
            if (runtimeProvider == null) {
                runtimeProvider = loadRuntimeProvider();
            }
            return runtimeProvider;
        }
        finally {
            LOCK.unlock();
        }
    }

    @GuardedBy("LOCK")
    private static StreamingLedgerRuntimeProvider loadRuntimeProvider() {
        try {
            ServiceLoader<StreamingLedgerRuntimeProvider> serviceLoader =
                    ServiceLoader.load(StreamingLedgerRuntimeProvider.class);

            Iterator<StreamingLedgerRuntimeProvider> iter = serviceLoader.iterator();

            // find the first service implementation
            StreamingLedgerRuntimeProvider firstProvider;
            if (iter.hasNext()) {
                firstProvider = iter.next();
            }
            else {
                throw new FlinkRuntimeException("No StreamingLedgerRuntimeProvider found. "
                        + "Please make sure you have a transaction runtime implementation in the classpath.");
            }

            // check if there is more than one service implementation
            if (iter.hasNext()) {
                String secondServiceName = "(could not load service implementation)";
                try {
                    secondServiceName = iter.next().getClass().getName();
                }
                catch (Throwable ignored) {
                }

                throw new FlinkRuntimeException("Ambiguous: Found more than one StreamingLedgerRuntimeProvider: "
                        + firstProvider.getClass().getName() + " and " + secondServiceName);
            }

            return firstProvider;
        }
        catch (FlinkRuntimeException e) {
            // simply propagate without further wrapping, for simplicity
            throw e;
        }
        catch (Throwable t) {
            throw new FlinkRuntimeException("Could not load StreamingLedgerRuntimeProvider", t);
        }
    }
}
