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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.dataartisans.streamingledger.sdk.api.StateAccess;
import com.dataartisans.streamingledger.sdk.api.StreamingLedger.StateAccessSpec;
import com.dataartisans.streamingledger.sdk.api.TransactionProcessFunction;
import com.dataartisans.streamingledger.sdk.api.TransactionProcessFunction.Context;
import com.dataartisans.streamingledger.sdk.api.TransactionProcessFunction.ProcessTransaction;
import com.dataartisans.streamingledger.sdk.api.TransactionProcessFunction.State;
import com.dataartisans.streamingledger.sdk.common.reflection.Methods;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * A {@link StreamingLedgerSpec} factory.
 */
@Internal
public final class StreamingLedgerSpecFactory {

    private StreamingLedgerSpecFactory() {
    }

    /**
     * Creates a new TransactionOperationSpec.
     *
     * @param <InT>     The type of the transaction data events.
     * @param <ResultT> The type of the transaction data results.
     */
    public static <InT, ResultT> StreamingLedgerSpec<InT, ResultT> create(
            TransactionProcessFunction<InT, ResultT> processFunction,
            List<StateAccessSpec<InT, ?, ?>> stateSpecs,
            TypeInformation<InT> inputType,
            TypeInformation<ResultT> outputType) {

        requireNonNull(processFunction);
        requireNonNull(stateSpecs);
        Iterator<Method> annotatedMethods = Methods.findAnnotatedMethods(
                processFunction.getClass(),
                ProcessTransaction.class);

        if (!annotatedMethods.hasNext()) {
            throw missingAnnotation(processFunction);
        }
        final Method method = annotatedMethods.next();
        if (annotatedMethods.hasNext()) {
            throw tooManyAnnotatedMethods(processFunction);
        }
        final Parameter[] parameters = method.getParameters();
        if (parameters.length != stateSpecs.size() + 2) {
            throw wrongParameterCount(processFunction, method.getName(), inputType.getTypeClass(), stateSpecs);
        }
        // first parameter is the input
        if (!isOfSimpleType(parameters[0], inputType)) {
            throw wrongParameter(processFunction, method.getName(), parameters[0], "wrong type.");
        }
        // second parameter is Context<ResultT>
        if (!isOfGenericType(parameters[1], Context.class, outputType)) {
            throw wrongParameter(processFunction, method.getName(), parameters[1], "wrong type.");
        }
        // the rest of the parameters are StateAccess<Value> annotated with the @State annotation.
        // The annotation connects a parameter to the StateAccessSpec<InT, Key, Value> via bindName.
        Map<String, StateAccessSpec<InT, ?, ?>> bindName2StateSpec = uniqueIndex(stateSpecs, s -> s.bindName);
        List<StateAccessSpec<InT, ?, ?>> referencedStateSpecs = new ArrayList<>();
        for (int i = 2; i < parameters.length; i++) {
            final Parameter parameter = parameters[i];
            final State state = parameter.getAnnotation(State.class);
            if (state == null) {
                throw wrongParameter(
                        processFunction,
                        method.getName(),
                        parameters[i],
                        "not annotated with a @State.");
            }
            StateAccessSpec<InT, ?, ?> stateAccess = bindName2StateSpec.get(state.value());
            if (stateAccess == null) {
                throw wrongParameter(
                        processFunction,
                        method.getName(),
                        parameters[i],
                        "unknown state spec '" + state.value() + "'");
            }
            // parameter: StateAccess<Value>
            if (!isOfGenericType(parameter, StateAccess.class, stateAccess.state.getValueType())) {
                throw wrongParameter(
                        processFunction,
                        method.getName(),
                        parameters[i],
                        "state spec '" + state.value() + "' has a value type "
                                + stateAccess.state.getValueType());
            }
            referencedStateSpecs.add(stateAccess);
        }

        return new StreamingLedgerSpec<>(
                processFunction,
                method.getName(),
                referencedStateSpecs,
                inputType,
                outputType);
    }


    // ------------------------------------------------------------------------
    //  Static helpers
    // ------------------------------------------------------------------------

    private static IllegalArgumentException missingAnnotation(Object subject) {
        String className = subject.getClass().getSimpleName();
        String annotation = ProcessTransaction.class.getSimpleName();
        return new IllegalArgumentException("Could not find any method of " + className + " that is annotated with @"
                + annotation
        );
    }

    private static IllegalArgumentException tooManyAnnotatedMethods(Object subject) {
        String className = subject.getClass().getSimpleName();
        String annotation = ProcessTransaction.class.getSimpleName();
        return new IllegalArgumentException("There multiple methods of " + className + " that are annotated with "
                + "@" + annotation
        );
    }

    private static IllegalArgumentException wrongParameter(
            Object subject,
            String methodName,
            Parameter parameter,
            String message) {
        String className = subject.getClass().getSimpleName();
        return new IllegalArgumentException("A problem with the field " + parameter + " of " + className + "."
                + methodName + "\t" + message
        );
    }

    private static <InT> IllegalArgumentException wrongParameterCount(
            Object subject,
            String methodName,
            Class<?> typeClass,
            List<StateAccessSpec<InT, ?, ?>> stateSpecs) {
        String className = subject.getClass().getSimpleName();
        return new IllegalArgumentException(className + "." + methodName + " has wrong argument count."
                + " Expected: " + methodName + "(" + typeClass + ", Context<" + typeClass + ">, ... "
                + stateSpecs.size() + " state accesses"
        );
    }

    private static boolean isOfSimpleType(Parameter parameter, TypeInformation<?> type) {
        return parameter.getType() == type.getTypeClass();
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private static boolean isOfGenericType(Parameter parameter, Class<?> baseType, TypeInformation<?> type) {
        if (!(parameter.getParameterizedType() instanceof ParameterizedType)) {
            return false;
        }
        ParameterizedType parameterizedType = (ParameterizedType) parameter.getParameterizedType();
        if (parameterizedType.getRawType() != baseType) {
            return false;
        }
        Type t = parameterizedType.getActualTypeArguments()[0];
        return t == type.getTypeClass();
    }

    private static <K, E> Map<K, E> uniqueIndex(Iterable<E> elements, Function<E, K> indexExtractor) {
        Map<K, E> key2element = new HashMap<>();
        for (E element : elements) {
            K key = indexExtractor.apply(element);
            key2element.put(key, element);
        }
        return key2element;
    }
}
