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

package com.dataartisans.streamingledger.sdk.common.reflection;

import com.dataartisans.streamingledger.sdk.api.StateAccess;
import com.dataartisans.streamingledger.sdk.api.TransactionProcessFunction;
import com.dataartisans.streamingledger.sdk.api.TransactionProcessFunction.Context;

/**
 * An auto-generated class that invokes a user provided {@link TransactionProcessFunction}.
 *
 * @param <InT>  transaction event type.
 * @param <OutT> transaction result type.
 */
public abstract class ProcessFunctionInvoker<InT, OutT> {

    public abstract void invoke(InT input, Context<OutT> context, StateAccess[] arguments);
}
