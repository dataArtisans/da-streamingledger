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

package com.dataartisans.streamingledger.sdk.common.union;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.List;

/**
 * UnionSerializerConfigSnapshot - A serializer configuration snapshot for the {@link UnionSerializer}.
 */
public final class UnionSerializerConfigSnapshot extends CompositeTypeSerializerConfigSnapshot {

    private static final int VERSION = 1;

    /**
     * A default constructor is required by Flink's serialization framework.
     */
    @SuppressWarnings("unused")
    public UnionSerializerConfigSnapshot() {
    }

    /**
     * Creates a new instance of UnionSerializerConfigSnapshot.
     */
    @SuppressWarnings("WeakerAccess")
    public UnionSerializerConfigSnapshot(List<TypeSerializer<?>> underlyingSerializers) {
        super(toArray(underlyingSerializers));
    }

    private static TypeSerializer<?>[] toArray(List<TypeSerializer<?>> underlyingSerializers) {
        final int n = underlyingSerializers.size();
        TypeSerializer<?>[] rawSerializers = new TypeSerializer[n];
        return underlyingSerializers.toArray(rawSerializers);
    }

    @Override
    public int getVersion() {
        return VERSION;
    }
}
