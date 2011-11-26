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
package org.springframework.batch.execution.aggregation.test;

import org.springframework.batch.execution.aggregation.core.AggregationItemListener;
import org.springframework.batch.execution.aggregation.core.AggregationTimeoutPolicy;

/**
 * A test {@link AggregationTimeoutPolicy} that also implements the
 * {@link AggregationItemListener} interface.
 *
 * @author Stephane Nicoll
 */
public class ExtendedAggregationTimeoutPolity implements AggregationTimeoutPolicy, AggregationItemListener {

    public void onItemRegistration(Object item) {

    }

    public boolean shouldTimeout(long startTime) {
        return false;
    }
}
