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
import org.springframework.batch.execution.aggregation.core.AggregationItemListener;
import org.springframework.batch.execution.aggregation.core.AggregationItemMapper;
import org.springframework.batch.execution.aggregation.core.AggregationTimeoutPolicy;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;

/**
 * A base builder for {@link org.springframework.batch.execution.aggregation.core.AggregationContext}
 *
 * @author Stephane Nicoll
 * @see BaseAggregationContext
 */
public abstract class BaseAggregationContextBuilder<M, T, B extends BaseAggregationContextBuilder<M, T, B>> {

    public static final long DEFAULT_RECEIVE_TIMEOUT = 1000L; // 1 sec

    private AggregationCompletionPolicy<?> completionPolicy;
    private AggregationTimeoutPolicy timeoutPolicy;
    private final List<AggregationItemListener<T>> aggregationItemListeners;
    private AggregationItemMapper<M, T> aggregationItemMapper;
    private long receiveTimeout = DEFAULT_RECEIVE_TIMEOUT;


    /**
     * Creates an empty instance.
     */
    protected BaseAggregationContextBuilder() {
        this.aggregationItemListeners = new ArrayList<AggregationItemListener<T>>();
    }

    /**
     * Specifies the {@link AggregationCompletionPolicy} to use. Note that the policy
     * is automatically registered as a item listener.
     *
     * @param completionPolicy the completion policy to use
     * @return the builder for method chaining
     */
    public B withCompletionPolicy(AggregationCompletionPolicy<?> completionPolicy) {
        this.completionPolicy = completionPolicy;
        return self();
    }

    /**
     * Specifies the {@link AggregationTimeoutPolicy} to use. Note that the policy
     * is automatically registered as an item listener if it implements the
     * {@link AggregationItemListener} interface.
     *
     * @param timeoutPolicy the timeout policy to use
     * @return the builder for method chaining
     */
    public B withTimeoutPolicy(AggregationTimeoutPolicy timeoutPolicy) {
        this.timeoutPolicy = timeoutPolicy;
        return self();
    }

    /**
     * Registers the specified {@link AggregationItemListener}. If the listener is already
     * registered, does nothing.
     *
     * @param listener the listener to register
     * @return the builder for method chaining
     */
    public B withAggregationItemListener(AggregationItemListener<T> listener) {
        if (!aggregationItemListeners.contains(listener)) {
            aggregationItemListeners.add(listener);
        }
        return self();
    }

    /**
     * Specifies the {@link AggregationItemMapper} to use.
     *
     * @param mapper the mapper to use
     * @return the builder for method chaining
     */
    public B withAggregationItemMapper(AggregationItemMapper<M, T> mapper) {
        this.aggregationItemMapper = mapper;
        return self();
    }

    /**
     * Specifies the timeout to use when receiving a message. Use 0 to never timeout which
     * can be dangerous in case the message never arrives.
     * <p/>
     * If not specified, the default timeout is one sec (1000 ms)
     *
     * @param receiveTimeout the receive timeout to use
     * @return the builder for method chaining
     */
    public B withReceiveTimeout(long receiveTimeout) {
        Assert.state(receiveTimeout >= 0, "receiveTimeout must be positive.");
        this.receiveTimeout = receiveTimeout;
        return self();
    }

    /**
     * Returns the instance of this builder, for method chaining purposes.
     *
     * @return this
     */
    protected abstract B self();

    /**
     * Initializes the fresh context with the content of the builder.
     * <p/>
     * Validates that each mandatory property is set.
     *
     * @param context the context to build
     */
    protected void doBuild(BaseAggregationContext<M, T> context) {
        Assert.notNull(completionPolicy, "completion policy could not be null.");
        Assert.notNull(timeoutPolicy, "timeout policy could not be null.");
        Assert.notNull(aggregationItemMapper, "the aggregation item mapper could not be null.");

        // Register the listeners if necessary
        final List<AggregationItemListener<T>> listeners =
                new ArrayList<AggregationItemListener<T>>(aggregationItemListeners);
        addAggregationItemListenerIfNecessary(listeners, completionPolicy);
        addAggregationItemListenerIfNecessary(listeners, timeoutPolicy);

        context.setCompletionPolicy(completionPolicy);
        context.setTimeoutPolicy(timeoutPolicy);
        context.setAggregationItemListeners(listeners);
        context.setAggregationItemMapper(aggregationItemMapper);
        context.setReceiveTimeout(receiveTimeout);
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
