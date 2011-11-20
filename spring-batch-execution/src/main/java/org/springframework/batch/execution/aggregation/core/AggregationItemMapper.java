/*
 * Copyright 2006-2007 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.batch.execution.aggregation.core;

/**
 * Maps a message to an aggregation item.
 *
 * @param <M> the type of the message, according to the underlying transport technology
 * @param <T> the type of the result (i.e. the type of the mapped object)
 * @author Stephane Nicoll
 */
public interface AggregationItemMapper<M, T> {

    /**
     * Maps a message to the item it contains.
     *
     * @param message the input message
     * @return the item contained in the message
     * @throws MessageConversionException if an error occurred while manipulating the message
     */
    T map(M message) throws MessageConversionException;

}
