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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.XORShiftRandom;

import java.util.ArrayList;
import java.util.List;

/**
 * Test {@link UnionSerializer}.
 */
public class UnionSerializerTest extends SerializerTestBase<TaggedElement> {

    @Override
    protected TypeSerializer<TaggedElement> createSerializer() {
        List<TypeSerializer<?>> serializers = new ArrayList<>();
        ExecutionConfig config = new ExecutionConfig();

        serializers.add(BasicTypeInfo.LONG_TYPE_INFO.createSerializer(config));
        serializers.add(BasicTypeInfo.STRING_TYPE_INFO.createSerializer(config));
        serializers.add(BasicTypeInfo.BOOLEAN_TYPE_INFO.createSerializer(config));

        return new UnionSerializer(serializers);
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<TaggedElement> getTypeClass() {
        return TaggedElement.class;
    }

    @Override
    protected TaggedElement[] getTestData() {
        XORShiftRandom random = new XORShiftRandom();

        TaggedElement[] data = new TaggedElement[100];
        for (int i = 0; i < data.length; i++) {
            final int tag = random.nextInt(3);
            final Object element;
            switch (tag) {
                case 0: {
                    element = random.nextLong();
                    break;
                }
                case 1: {
                    byte[] bytes = new byte[random.nextInt(256)];
                    random.nextBytes(bytes);
                    element = new String(bytes);
                    break;
                }
                case 2: {
                    element = random.nextBoolean();
                    break;
                }
                default: {
                    element = null;
                }
            }
            data[i] = new TaggedElement(tag, element);
        }
        return data;
    }
}
