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

import com.dataartisans.streamingledger.sdk.api.TransactionProcessFunction;
import com.dataartisans.streamingledger.sdk.api.TransactionProcessFunction.ProcessTransaction;
import com.dataartisans.streamingledger.sdk.spi.StreamingLedgerSpec;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.NamingStrategy;
import net.bytebuddy.TypeCache;
import net.bytebuddy.TypeCache.Sort;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.method.MethodDescription.ForLoadedConstructor;
import net.bytebuddy.description.method.MethodDescription.ForLoadedMethod;
import net.bytebuddy.description.modifier.FieldManifestation;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.description.type.TypeDefinition;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.description.type.TypeDescription.Generic;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.DynamicType.Builder;
import net.bytebuddy.dynamic.DynamicType.Unloaded;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy.Default;
import net.bytebuddy.dynamic.scaffold.subclass.ConstructorStrategy;
import net.bytebuddy.implementation.FieldAccessor;
import net.bytebuddy.implementation.MethodCall;
import net.bytebuddy.implementation.bytecode.assign.Assigner;
import net.bytebuddy.implementation.bytecode.assign.Assigner.Typing;
import net.bytebuddy.matcher.ElementMatchers;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;

/**
 * A {@link ByteBuddy} backed factory of {@link ProcessFunctionInvoker}.
 */
public class ByteBuddyProcessFunctionInvoker {

    private static final TypeCache<StreamingLedgerSpec<?, ?>> CACHE = new TypeCache<>(Sort.SOFT);

    public static <InT, OutT> ProcessFunctionInvoker<InT, OutT> create(StreamingLedgerSpec<InT, OutT> spec) {
        final ClassLoader userClassLoader = classLoader(spec);
        final Class<?> generatedClass = CACHE.findOrInsert(userClassLoader, spec, () -> generateAndLoadClass(spec));

        try {
            return createInstance(generatedClass, spec);
        }
        catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new RuntimeException("Unable to create a new instance for " + spec.processFunction, e);
        }
    }

    private static <InT, OutT> Class<? extends ProcessFunctionInvoker<InT, OutT>> generateAndLoadClass(
            StreamingLedgerSpec<InT, OutT> spec) throws NoSuchMethodException {
        Unloaded<?> unloaded = createDynamicTypeFromSpec(spec);
        return loadClass(unloaded, classLoader(spec));
    }

    private static <InT, OutT> ProcessFunctionInvoker<InT, OutT> createInstance(
            Class<?> generatedClass,
            StreamingLedgerSpec<InT, OutT> spec)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

        Constructor<?> constructor = generatedClass.getDeclaredConstructor(spec.processFunction.getClass());
        constructor.setAccessible(true);

        @SuppressWarnings("unchecked")
        ProcessFunctionInvoker<InT, OutT> instance = (ProcessFunctionInvoker<InT, OutT>)
                constructor.newInstance(spec.processFunction);

        return instance;
    }

    private static <InT, OutT> DynamicType.Unloaded<?> createDynamicTypeFromSpec(StreamingLedgerSpec<InT, OutT> spec)
            throws NoSuchMethodException {
        PackageLocalNamingStrategy generatedTypeName = new PackageLocalNamingStrategy(spec.processFunction.getClass());

        TypeDefinition generatedType = Generic.Builder.parameterizedType(
                ProcessFunctionInvoker.class,
                spec.inputType.getTypeClass(),
                spec.resultType.getTypeClass()
        ).build();

        TypeDefinition processFunctionType = new TypeDescription.ForLoadedType(spec.processFunction.getClass());
        ForLoadedConstructor superTypeConstructor =
                new ForLoadedConstructor(ProcessFunctionInvoker.class.getDeclaredConstructor());
        MethodDescription processMethodType = processMethodTypeFromSpec(spec);

        Builder<?> builder = configureByteBuddyBuilder(
                generatedTypeName,
                generatedType,
                processFunctionType,
                superTypeConstructor,
                processMethodType,
                spec.stateBindings.size());

        return builder.make();
    }

    private static Builder<?> configureByteBuddyBuilder(
            PackageLocalNamingStrategy generatedTypeName,
            TypeDefinition generatedType,
            TypeDefinition processFunctionType,
            ForLoadedConstructor superTypeConstructor,
            MethodDescription processMethodType,
            int numberOfStateBindings) {

        return new ByteBuddy()
                // final class <Name> extends <ProcessFunctionInvoker> {
                .with(generatedTypeName)
                .subclass(generatedType, ConstructorStrategy.Default.NO_CONSTRUCTORS).modifiers(Modifier.FINAL)
                // private final <processFunction class> delegate;
                .defineField("delegate", processFunctionType, Visibility.PRIVATE, FieldManifestation.FINAL)
                // public <Name>(<processFunction class> delegate) {
                //     super();
                //     this.delegate = delegate;
                // }
                .defineConstructor(Modifier.PUBLIC)
                .withParameters(processFunctionType)
                .intercept(MethodCall.invoke(superTypeConstructor)
                        .andThen(FieldAccessor.ofField("delegate").setsArgumentAt(0))
                )
                // invoke(input, context, StateAccess[] arguments) {
                //      this.delegate.invoke(input, context, arguments[0], arguments[1], .. arguments[n - 1]);
                // }
                .method(ElementMatchers.named("invoke"))
                .intercept(MethodCall.invoke(processMethodType)
                        .onField("delegate")
                        .withArgument(0, 1) // event & context
                        .withArgumentArrayElements(2, numberOfStateBindings) // StateAccess
                        .withAssigner(Assigner.DEFAULT, Typing.STATIC)
                );
    }

    private static <InT, OutT> MethodDescription processMethodTypeFromSpec(StreamingLedgerSpec<InT, OutT> spec) {
        Class<? extends TransactionProcessFunction> processClass = spec.processFunction.getClass();
        Iterator<Method> methods = Methods.findAnnotatedMethods(processClass, ProcessTransaction.class);
        if (!methods.hasNext()) {
            throw new IllegalArgumentException("Unable to find an annotated method on " + processClass.getSimpleName());
        }
        Method method = methods.next();
        if (methods.hasNext()) {
            throw new IllegalArgumentException(
                    "Was expecting a single method annotated with a ProcessTransaction, but found more.");
        }
        return new ForLoadedMethod(method);
    }

    @SuppressWarnings("unchecked")
    private static <InT, OutT> Class<? extends ProcessFunctionInvoker<InT, OutT>> loadClass(
            DynamicType.Unloaded<?> unloaded,
            ClassLoader classLoader) {
        return (Class<? extends ProcessFunctionInvoker<InT, OutT>>)
                unloaded
                        .load(classLoader, Default.INJECTION)
                        .getLoaded();
    }

    private static <InT, OutT> ClassLoader classLoader(StreamingLedgerSpec<InT, OutT> spec) {
        final Class<? extends TransactionProcessFunction> userClass = spec.processFunction.getClass();
        return userClass.getClassLoader();
    }

    /**
     * A naming strategy for generated classes.
     *
     * <p>The following name format is produced: user-package . superClass $ user-className $ sequence number
     */
    private static final class PackageLocalNamingStrategy extends NamingStrategy.AbstractBase {
        private static final AtomicLong sequence = new AtomicLong();
        private final String packageName;
        private final String className;

        PackageLocalNamingStrategy(Class<?> superType) {
            requireNonNull(superType);
            Package aPackage = superType.getPackage();
            if (aPackage == null) {
                this.packageName = "";
            }
            else {
                this.packageName = aPackage.getName();
            }
            this.className = superType.getSimpleName();
        }

        @Override
        protected String name(TypeDescription superClass) {
            StringBuilder sb = new StringBuilder();
            if (!packageName.isEmpty()) {
                sb.append(packageName);
                sb.append('.');
            }
            sb.append(superClass.getSimpleName());
            sb.append("$");
            sb.append(className);
            sb.append("$");
            sb.append(sequence.incrementAndGet());
            return sb.toString();
        }
    }

}
