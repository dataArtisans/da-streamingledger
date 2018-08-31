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
 * Base exception for failures during a state access.
 *
 * <p>This is an unchecked exception, because instances of this exception are raised as a result
 * of an incorrect program, for example when read and write accesses are incorrectly declared.
 * Instances of this exception are hence rarely expected to be handled explicitly within an user's
 * implementation of an application, but rather expected to "bubble up" and report a proper
 * application failure.
 */
public class StateAccessException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public StateAccessException(String message) {
        super(message);
    }

    public StateAccessException(String message, Throwable cause) {
        super(message, cause);
    }
}
