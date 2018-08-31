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

import java.util.List;
import java.util.Objects;

/**
 * A simple POJO with a dataStreamTag and an element.
 *
 * <p>see {@link Union#apply(List)}.
 */
public final class TaggedElement {

    public static final int UNDEFINED_TAG = -1;

    /**
     * an index of the original data stream before the union.
     */
    private int dataStreamTag;

    /**
     * an element from one of the original data streams.
     */
    private Object element;

    public TaggedElement(int dataStreamTag, Object element) {
        this.dataStreamTag = dataStreamTag;
        this.element = element;
    }

    public int getDataStreamTag() {
        return dataStreamTag;
    }

    public void setDataStreamTag(int dataStreamTag) {
        this.dataStreamTag = dataStreamTag;
    }

    public Object getElement() {
        return element;
    }

    public void setElement(Object element) {
        this.element = element;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TaggedElement that = (TaggedElement) o;
        return dataStreamTag == that.dataStreamTag
                && Objects.equals(element, that.element);
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = 31 * result + (element == null ? 0 : element.hashCode());
        result = 31 * result + Integer.hashCode(dataStreamTag);
        return result;
    }
}
