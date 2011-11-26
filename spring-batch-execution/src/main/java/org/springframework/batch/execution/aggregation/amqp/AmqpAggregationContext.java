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
package org.springframework.batch.execution.aggregation.amqp;

import com.rabbitmq.client.Channel;
import org.springframework.amqp.core.Message;
import org.springframework.batch.execution.aggregation.core.AggregationContext;

/**
 * Holds the requested information to perform the aggregation using AMQP.
 *
 * @author Stephane Nicoll
 */
public interface AmqpAggregationContext<T> extends AggregationContext<Message, T> {

    /**
     * Returns the {@link Channel} to use to receive the aggregation items.
     *
     * @return the channel to use
     */
    Channel getChannel();

    /**
     * Returns the destination to use to listen for aggregation item.
     *
     * @return the destination
     */
    String getDestination();

    /**
     * Returns the encoding to use for incoming messages.
     *
     * @return the encoding to use
     */
    String getEncoding();

}


