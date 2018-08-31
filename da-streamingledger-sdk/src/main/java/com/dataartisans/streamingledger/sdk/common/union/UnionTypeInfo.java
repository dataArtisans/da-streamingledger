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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

final class UnionTypeInfo extends TypeInformation<TaggedElement> implements Serializable {

    private static final long serialVersionUID = 1;

    private final List<TypeInformation<?>> underlyingTypes;

    UnionTypeInfo(List<TypeInformation<?>> underlyingTypes) {
        requireNonNull(underlyingTypes);
        this.underlyingTypes = underlyingTypes;
    }

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 1;
    }

    @Override
    public int getTotalFields() {
        return 1;
    }

    @Override
    public Class<TaggedElement> getTypeClass() {
        return TaggedElement.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<TaggedElement> createSerializer(ExecutionConfig config) {
        List<TypeSerializer<?>> underlyingSerializers = new ArrayList<>(underlyingTypes.size());
        for (TypeInformation<?> underlyingType : underlyingTypes) {
            TypeSerializer<?> serializer = underlyingType.createSerializer(config);
            underlyingSerializers.add(serializer);
        }
        return new UnionSerializer(underlyingSerializers);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("UnionTaggedSerializer {");
        final int size = underlyingTypes.size();
        for (int i = 0; i < size; i++) {
            sb.append(underlyingTypes.get(i).toString());
            if (i < size - 1) {
                sb.append(", ");
            }
        }
        sb.append(" }");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UnionTypeInfo that = (UnionTypeInfo) o;
        return Objects.equals(underlyingTypes, that.underlyingTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(underlyingTypes);
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof UnionTypeInfo;
    }
}
