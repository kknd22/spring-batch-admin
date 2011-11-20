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

import java.util.Collection;

/**
 * The context used by the aggregation service.
 *
 * @author Stephane Nicoll
 */
public interface AggregationContext<M, T> {

    /**
     * Returns the completion policy to use.
     *
     * @return the completion policy
     */
    AggregationCompletionPolicy getCompletionPolicy();

    /**
     * Returns the timeout policy to use.
     *
     * @return the timeout policy
     */
    AggregationTimeoutPolicy getTimeoutPolicy();

    /**
     * Returns the registered {@link AggregationItemListener} implementations.
     *
     * @return the aggregation item listeners
     */
    Collection<AggregationItemListener<T>> getAggregationItemListeners();

    /**
     * Returns the aggregation item mapper to use.
     *
     * @return the aggregation item mapper
     */
    AggregationItemMapper<M, T> getAggregationItemMapper();
}
