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

import org.springframework.util.Assert;

import javax.jms.Destination;
import javax.jms.Session;
import java.util.ArrayList;
import java.util.List;

/**
 * Builds {@link JmsAggregationContext} instances.
 *
 * @author Stephane Nicoll
 */
public final class JmsAggregationContextBuilder<T> {

    public static final long DEFAULT_RECEIVE_TIMEOUT = 1000L; // 1 minute

    private AggregationCompletionPolicy<?> completionPolicy;
    private AggregationTimeoutPolicy timeoutPolicy;
    private final List<AggregationItemListener<T>> aggregationItemListeners;
    private AggregationItemJmsMapper<T> aggregationItemJmsMapper;
    private Session session;
    private Destination destination;
    private long receiveTimeout = DEFAULT_RECEIVE_TIMEOUT;

    private JmsAggregationContextBuilder(Session session, Destination destination) {
        Assert.notNull(session, "session could not be null.");
        Assert.notNull(destination, "destination could not be null.");
        this.session = session;
        this.destination = destination;
        this.aggregationItemListeners = new ArrayList<AggregationItemListener<T>>();
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
     * Specifies the {@link AggregationCompletionPolicy} to use. Note that the policy
     * is automatically registered as a item listener.
     *
     * @param completionPolicy the completion policy to use
     * @return the builder for method chaining
     */
    public JmsAggregationContextBuilder<T> withCompletionPolicy(AggregationCompletionPolicy<?> completionPolicy) {
        this.completionPolicy = completionPolicy;
        return this;
    }

    /**
     * Specifies the {@link AggregationTimeoutPolicy} to use. Note that the policy
     * is automatically registered as an item listener if it implements the
     * {@link AggregationItemListener} interface.
     *
     * @param timeoutPolicy the timeout policy to use
     * @return the builder for method chaining
     */
    public JmsAggregationContextBuilder<T> withTimeoutPolicy(AggregationTimeoutPolicy timeoutPolicy) {
        this.timeoutPolicy = timeoutPolicy;
        return this;
    }

    /**
     * Registers the specified {@link AggregationItemListener}. If the listener is already
     * registered, does nothing.
     *
     * @param listener the listener to register
     * @return the builder for method chaining
     */
    public JmsAggregationContextBuilder<T> withAggregationItemListener(AggregationItemListener<T> listener) {
        if (!aggregationItemListeners.contains(listener)) {
            aggregationItemListeners.add(listener);
        }
        return this;
    }

    /**
     * Specifies the {@link AggregationItemJmsMapper} to use.
     *
     * @param mapper the mapper to use
     * @return the builder for method chaining
     */
    public JmsAggregationContextBuilder<T> withAggregationItemMapper(AggregationItemJmsMapper<T> mapper) {
        this.aggregationItemJmsMapper = mapper;
        return this;
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
     * Builds the context. Validates that each mandatory property is set.
     *
     * @return the context
     */
    public JmsAggregationContext<T> build() {
        Assert.notNull(completionPolicy, "completion policy could not be null.");
        Assert.notNull(timeoutPolicy, "timeout policy could not be null.");
        Assert.notNull(aggregationItemJmsMapper, "the aggregation item mapper could not be null.");

        // Register the listeners if necessary
        final List<AggregationItemListener<T>> listeners =
                new ArrayList<AggregationItemListener<T>>(aggregationItemListeners);
        addAggregationItemListenerIfNecessary(listeners, completionPolicy);
        addAggregationItemListenerIfNecessary(listeners, timeoutPolicy);

        final JmsAggregationContextImpl<T> context = new JmsAggregationContextImpl<T>();
        context.setSession(session);
        context.setDestination(destination);
        context.setCompletionPolicy(completionPolicy);
        context.setTimeoutPolicy(timeoutPolicy);
        context.setAggregationItemListeners(listeners);
        context.setAggregationItemJmsMapper(aggregationItemJmsMapper);
        context.setReceiveTimeout(receiveTimeout);

        return context;
    }

    @SuppressWarnings({"unchecked"})
    private void addAggregationItemListenerIfNecessary(List<AggregationItemListener<T>> listeners, Object o) {
        if (o instanceof AggregationItemListener) {
            final AggregationItemListener<T> listener = (AggregationItemListener<T>) o;
            if (!listeners.contains(listener)) {
                listeners.add(listener);
            }
        }
    }

}
