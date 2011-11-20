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
package org.springframework.batch.execution.aggregation.jms;

import org.springframework.batch.execution.aggregation.core.AggregationContext;

import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.Session;

/**
 * Holds the requested information to perform the aggregation.
 *
 * @author Stephane Nicoll
 * @see JmsAggregationService
 */
public interface JmsAggregationContext<T> extends AggregationContext<Message, T> {

    /**
     * Returns the session to use to receive the aggregation items.
     *
     * @return the session to use
     */
    Session getSession();

    /**
     * Returns the {@link javax.jms.Destination} to use to listen for aggregation item.
     *
     * @return the destination
     */
    Destination getDestination();

    /**
     * Returns the receive timeout to use when listening for incoming messages.
     *
     * @return the receive timeout, in ms
     */
    long getReceiveTimeout();

}
