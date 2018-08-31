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

/**
 * The AccessType defines whether certain state is only read or written or both.
 *
 * <p>The type of access has an impact on whether accesses to the state (and key) by different
 * transactions can execute independent of each other, or whether they need to be coordinated.
 * For example, multiple transactions can concurrently read a certain state, if no other
 * state currently writes the state.
 */
@SuppressWarnings("unused")
public enum AccessType {

    /**
     * Indicates that state is only read, but not modified.
     * If state is only read, it can never block any other transaction from advancing.
     */
    READ(false, false),

    /**
     * Indicates that state is modified, but not read before. Modifying state means that coordination
     * is involved to ensure consistency for the write back.
     */
    WRITE(true, true),

    /**
     * Indicates that state is read and later modified. This access type involves coordination to
     * ensure consistency for the write back, and possibly additional messages to read state.
     */
    READ_WRITE(true, false);

    private final boolean requiresLocking;
    private final boolean writeOnly;

    AccessType(boolean requiresLocking, boolean writeOnly) {
        this.requiresLocking = requiresLocking;
        this.writeOnly = writeOnly;
    }

    public boolean requiresLocking() {
        return requiresLocking;
    }

    public boolean writeOnly() {
        return writeOnly;
    }
}
