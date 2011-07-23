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
package org.springframework.batch.execution.jms.partition.aggregation;

/**
 * Policy to determine if a timeout has been reached while aggregating
 * some data.
 * <p/>
 * Implementation willing to get an overview of the item(s) that have
 * already been received may want to implement {@link AggregationItemListener}
 * as well.
 *
 * @author Stephane Nicoll
 * @see AggregationItemListener
 */
public interface AggregationTimeoutPolicy {

    /**
     * Specifies if the aggregation has gone in timeout, based on the time
     * at which it started.
     *
     * @param startTime the time at which aggregation started
     * @return <tt>true</tt> if aggregation should timeout
     */
    boolean shouldTimeout(long startTime);

}
