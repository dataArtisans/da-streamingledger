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

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Stream;

/**
 * Reflection utility functions.
 */
public final class Methods {
    private Methods() {
    }

    /**
     * Finds all the {@link Method}s that are annotated with the supplied annotation.
     *
     * <p>This method will traverse up the superclass hierarchy looking for methods annotated with the supplied
     * annotation.
     *
     * @return an iterator of {@link Method}s found.
     */
    public static Iterator<Method> findAnnotatedMethods(Class<?> javaClass, Class<? extends Annotation> annotation) {
        return definedMethods(javaClass)
                .filter(method -> method.getAnnotation(annotation) != null)
                .iterator();
    }

    private static Stream<Method> definedMethods(Class<?> javaClass) {
        if (javaClass == null || javaClass == Object.class) {
            return Stream.empty();
        }
        Stream<Method> selfMethods = Arrays.stream(javaClass.getDeclaredMethods());
        Stream<Method> superMethods = definedMethods(javaClass.getSuperclass());

        return Stream.concat(selfMethods, superMethods);
    }
}
