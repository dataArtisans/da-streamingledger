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

import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.CompatibilityUtil;
import org.apache.flink.api.common.typeutils.TypeDeserializer;
import org.apache.flink.api.common.typeutils.TypeDeserializerAdapter;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.api.common.typeutils.CompatibilityResult.requiresMigration;
import static org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.base.Preconditions.checkArgument;

final class UnionSerializer extends TypeSerializer<TaggedElement> {

    private static final long serialVersionUID = 1;

    private final TypeSerializer<Object>[] underlyingSerializers;

    private transient Object[] reusableObjects;

    UnionSerializer(List<TypeSerializer<?>> underlyingSerializers) {
        requireNonNull(underlyingSerializers);
        checkArgument(!underlyingSerializers.isEmpty(), "At least one underlying serializer is needed.");
        this.underlyingSerializers = toArray(underlyingSerializers);
        this.reusableObjects = createReusableObjects(this.underlyingSerializers);
    }

    private static Object[] createReusableObjects(TypeSerializer<Object>[] underlyingSerializers) {
        Object[] reusableObjects = new Object[underlyingSerializers.length];
        for (int i = 0; i < reusableObjects.length; i++) {
            reusableObjects[i] = underlyingSerializers[i].createInstance();
        }
        return reusableObjects;
    }

    @SuppressWarnings("unchecked")
    private static TypeSerializer<Object>[] toArray(List<TypeSerializer<?>> underlyingSerializers) {
        return underlyingSerializers.toArray(new TypeSerializer[0]);
    }

    @Override
    public boolean isImmutableType() {
        for (TypeSerializer<?> serializer : underlyingSerializers) {
            if (!serializer.isImmutableType()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public TypeSerializer<TaggedElement> duplicate() {
        List<TypeSerializer<?>> duplicates = new ArrayList<>(underlyingSerializers.length);
        boolean stateful = false;
        for (TypeSerializer<?> serializer : underlyingSerializers) {
            TypeSerializer<?> duplicate = serializer.duplicate();
            if (duplicate != serializer) {
                stateful = true;
            }
            duplicates.add(duplicate);
        }
        if (!stateful) {
            return this;
        }
        return new UnionSerializer(duplicates);
    }

    @Override
    public TaggedElement createInstance() {
        return new TaggedElement(TaggedElement.UNDEFINED_TAG, null);
    }

    @Override
    public TaggedElement copy(TaggedElement from) {
        final int tag = from.getDataStreamTag();
        Object copyOf = underlyingSerializers[tag].copy(from.getElement());
        return new TaggedElement(from.getDataStreamTag(), copyOf);
    }

    @Override
    public TaggedElement copy(TaggedElement from, TaggedElement reuse) {
        final int tag = from.getDataStreamTag();
        final TypeSerializer<Object> serializer = underlyingSerializers[tag];
        final Object elementCopy = serializer.copy(from.getElement(), reusableObjects[tag]);

        reuse.setElement(elementCopy);
        reuse.setDataStreamTag(tag);
        return reuse;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(TaggedElement record, DataOutputView target) throws IOException {
        final int tag = record.getDataStreamTag();
        target.writeInt(tag);
        underlyingSerializers[tag].serialize(record.getElement(), target);
    }

    @Override
    public TaggedElement deserialize(DataInputView source) throws IOException {
        final int tag = source.readInt();
        Object value = underlyingSerializers[tag].deserialize(source);
        return new TaggedElement(tag, value);
    }

    @Override
    public TaggedElement deserialize(TaggedElement reuse, DataInputView source) throws IOException {
        final int tag = source.readInt();
        final TypeSerializer<Object> serializer = underlyingSerializers[tag];
        final Object element = serializer.deserialize(reusableObjects[tag], source);

        reuse.setDataStreamTag(tag);
        reuse.setElement(element);
        return reuse;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        final int tag = source.readInt();
        target.writeInt(tag);
        underlyingSerializers[tag].copy(source, target);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UnionSerializer that = (UnionSerializer) o;
        return Arrays.equals(underlyingSerializers, that.underlyingSerializers);
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof UnionSerializer;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(underlyingSerializers);
    }

    // -----------------------------------------------------------------------------------
    // Internal helper methods
    // -----------------------------------------------------------------------------------

    @Override
    public TypeSerializerConfigSnapshot snapshotConfiguration() {
        return new UnionSerializerConfigSnapshot(Arrays.asList(underlyingSerializers));
    }

    @Override
    public CompatibilityResult<TaggedElement> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
        if (!(configSnapshot instanceof UnionSerializerConfigSnapshot)) {
            return requiresMigration();
        }
        UnionSerializerConfigSnapshot config = (UnionSerializerConfigSnapshot) configSnapshot;
        List<Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>> previousSerializersAndConfigs =
                config.getNestedSerializersAndConfigs();

        if (previousSerializersAndConfigs.size() != underlyingSerializers.length) {
            return requiresMigration();
        }

        List<TypeSerializer<?>> migratedSerializers = new ArrayList<>();
        // Adopted from the EitherSerializerConfigSnapshot.
        // One (or more) of the serializers here is not compatible with one (or more) of the previous serializers.
        boolean requiresMigration = false;
        for (int i = 0; i < underlyingSerializers.length; i++) {
            final TypeSerializer<?> underlyingSerializer = underlyingSerializers[i];

            final CompatibilityResult<?> res = CompatibilityUtil.resolveCompatibilityResult(
                    previousSerializersAndConfigs.get(i).f0,
                    UnloadableDummyTypeSerializer.class,
                    previousSerializersAndConfigs.get(i).f1,
                    underlyingSerializer);

            if (!res.isRequiresMigration()) {
                migratedSerializers.add(underlyingSerializer);
                continue;
            }
            requiresMigration = true;
            TypeDeserializer<?> deserializer = res.getConvertDeserializer();
            if (deserializer == null) {
                return requiresMigration();
            }
            migratedSerializers.add(new TypeDeserializerAdapter<>(deserializer));
        }
        UnionSerializer migratedSerializer = new UnionSerializer(migratedSerializers);
        return requiresMigration ? requiresMigration(migratedSerializer) : CompatibilityResult.compatible();
    }

    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        this.reusableObjects = createReusableObjects(this.underlyingSerializers);
    }
}
