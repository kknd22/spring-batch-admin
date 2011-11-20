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
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link org.springframework.batch.execution.aggregation.core.AggregationCompletionPolicy} implementation that stops after having
 * received a configurable amount of items.
 *
 * @author Stephane Nicoll
 */
public class CountBasedAggregationCompletionPolicy
        implements AggregationCompletionPolicy<Object> {

    private final List<Object> items = new ArrayList<Object>();
    private final int count;

    /**
     * Creates a new instance with the specified number of expected items.
     *
     * @param count the number of items to reach completion
     */
    public CountBasedAggregationCompletionPolicy(int count) {
        Assert.state(count > 0, "count should be higher than zero.");
        this.count = count;
    }

    /**
     * Returns the number of expected items.
     *
     * @return the number of expected items
     */
    public int getCount() {
        return count;
    }

    public boolean shouldComplete() {
        return items.size() == count;
    }

    public void onItemRegistration(Object item) {
        Assert.notNull(item, "Registered item could not be null.");
        items.add(item);
    }

}
