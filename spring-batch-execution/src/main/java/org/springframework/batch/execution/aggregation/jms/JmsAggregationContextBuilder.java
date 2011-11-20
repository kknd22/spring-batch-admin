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

import org.springframework.batch.execution.aggregation.core.support.BaseAggregationContextBuilder;
import org.springframework.util.Assert;

import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.Session;

/**
 * Builds {@link JmsAggregationContext} instances.
 *
 * @author Stephane Nicoll
 */
public final class JmsAggregationContextBuilder<T>
        extends BaseAggregationContextBuilder<Message, T, JmsAggregationContextBuilder<T>> {

    public static final long DEFAULT_RECEIVE_TIMEOUT = 1000L; // 1 minute

    private Session session;
    private Destination destination;
    private long receiveTimeout = DEFAULT_RECEIVE_TIMEOUT;

    private JmsAggregationContextBuilder(Session session, Destination destination) {
        Assert.notNull(session, "session could not be null.");
        Assert.notNull(destination, "destination could not be null.");
        this.session = session;
        this.destination = destination;
    }

    @Override
    protected JmsAggregationContextBuilder<T> self() {
        return this;
    }

    /**
     * Creates a new builder for the specified {@link javax.jms.Destination}. Incoming messages
     * are expected to arrive on that destination.
     *
     * @param resultType the type of the result
     * @param session the session to use to create the consumer
     * @param destination the incoming destination
     * @return the builder
     */
    public static <T> JmsAggregationContextBuilder<T> forDestination(Class<T> resultType,
                                                                     Session session, Destination destination) {
        return new JmsAggregationContextBuilder<T>(session, destination);
    }

    /**
     * Specifies the timeout to use when receiving a jms message. Use 0 to never timeout which
     * can be dangerous in case the message never arrives.
     * <p/>
     * If not specified, the default timeout is one sec (1000 ms)
     *
     * @param receiveTimeout the receive timeout to use
     * @return the builder for method chaining
     */
    public JmsAggregationContextBuilder<T> withReceiveTimeout(long receiveTimeout) {
        Assert.state(receiveTimeout >= 0, "receiveTimeout must be positive.");
        this.receiveTimeout = receiveTimeout;
        return this;
    }

    /**
     * Builds the context.
     *
     * @return the context
     */
    public JmsAggregationContext<T> build() {
        final JmsAggregationContextImpl<T> context = new JmsAggregationContextImpl<T>();
        doBuild(context);

        context.setSession(session);
        context.setDestination(destination);
        context.setReceiveTimeout(receiveTimeout);

        return context;
    }

}
