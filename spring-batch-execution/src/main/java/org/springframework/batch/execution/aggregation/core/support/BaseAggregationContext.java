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
package org.springframework.batch.execution.aggregation.core.support;

import org.springframework.batch.execution.aggregation.core.AggregationCompletionPolicy;
import org.springframework.batch.execution.aggregation.core.AggregationContext;
import org.springframework.batch.execution.aggregation.core.AggregationItemListener;
import org.springframework.batch.execution.aggregation.core.AggregationItemMapper;
import org.springframework.batch.execution.aggregation.core.AggregationTimeoutPolicy;

import java.util.Collection;

/**
 * A basic aggregation context.
 *
 * @author Stephane Nicoll
 */
public class BaseAggregationContext<M, T> implements AggregationContext<M, T> {

    private AggregationCompletionPolicy completionPolicy;
    private AggregationTimeoutPolicy timeoutPolicy;
    private Collection<AggregationItemListener<T>> aggregationItemListeners;
    private AggregationItemMapper<M, T> aggregationItemMapper;

    public AggregationCompletionPolicy getCompletionPolicy() {
        return completionPolicy;
    }

    public void setCompletionPolicy(AggregationCompletionPolicy<?> completionPolicy) {
        this.completionPolicy = completionPolicy;
    }

    public AggregationTimeoutPolicy getTimeoutPolicy() {
        return timeoutPolicy;
    }

    public void setTimeoutPolicy(AggregationTimeoutPolicy timeoutPolicy) {
        this.timeoutPolicy = timeoutPolicy;
    }

    public Collection<AggregationItemListener<T>> getAggregationItemListeners() {
        return aggregationItemListeners;
    }

    public void setAggregationItemListeners(Collection<AggregationItemListener<T>> aggregationItemListeners) {
        this.aggregationItemListeners = aggregationItemListeners;
    }

    public AggregationItemMapper<M, T> getAggregationItemMapper() {
        return aggregationItemMapper;
    }

    public void setAggregationItemMapper(AggregationItemMapper<M, T> aggregationItemMapper) {
        this.aggregationItemMapper = aggregationItemMapper;
    }

}
