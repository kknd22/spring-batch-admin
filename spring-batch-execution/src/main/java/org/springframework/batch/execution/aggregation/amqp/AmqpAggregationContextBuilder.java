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
import org.springframework.batch.execution.aggregation.core.support.BaseAggregationContextBuilder;
import org.springframework.util.Assert;

/**
 * Builds {@link AmqpAggregationContext} instances.
 *
 * @author Stephane Nicoll
 */
public class AmqpAggregationContextBuilder<T>
        extends BaseAggregationContextBuilder<Message, T, AmqpAggregationContextBuilder<T>> {

    public static final String DEFAULT_ENCODING = "UTF-8";

    private final Channel channel;
    private final String destination;
    private String encoding = DEFAULT_ENCODING;

    private AmqpAggregationContextBuilder(Channel channel, String destination) {
        Assert.notNull(channel, "channel could not be null.");
        Assert.notNull(destination, "destination could not be null.");
        this.channel = channel;
        this.destination = destination;
    }

    @Override
    protected AmqpAggregationContextBuilder<T> self() {
        return this;
    }

    /**
     * Creates a new builder for the specified destination. Incoming messages
     * are expected to arrive on that destination.
     *
     * @param resultType the type of the result
     * @param channel the channel to use to create the consumer
     * @param destination the incoming destination
     * @return the builder
     */
    public static <T> AmqpAggregationContextBuilder<T> forDestination(Class<T> resultType,
                                                                      Channel channel, String destination) {
        return new AmqpAggregationContextBuilder<T>(channel, destination);
    }

    /**
     * Specifies the encoding to use when receiving a message.
     * <p/>
     * If not specified, the default encoding is used
     *
     * @param encoding the encoding to use
     * @return the builder for method chaining
     * @see #DEFAULT_ENCODING
     */
    public AmqpAggregationContextBuilder<T> withEncoding(String encoding) {
        Assert.notNull(encoding, "encoding could not be null.");
        this.encoding = encoding;
        return this;
    }

    /**
     * Builds the context.
     *
     * @return the context
     */
    public AmqpAggregationContext<T> build() {
        final AmqpAggregationContextImpl<T> context = new AmqpAggregationContextImpl<T>();
        doBuild(context);

        context.setChannel(channel);
        context.setDestination(destination);
        context.setEncoding(encoding);

        return context;
    }

}
