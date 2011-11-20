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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.listener.OrderedComposite;
import org.springframework.batch.execution.aggregation.core.AggregationItemListener;
import org.springframework.jms.listener.adapter.ListenerExecutionFailedException;
import org.springframework.jms.support.JmsUtils;
import org.springframework.jms.support.converter.MessageConversionException;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * A message aggregator, based on a context which basically waits for incoming items
 * on a given destination. Uses different policy to determine if the aggregation is
 * done or should go in timeout.
 *
 * @author Stephane Nicoll
 * @see org.springframework.batch.execution.aggregation.core.AggregationCompletionPolicy
 * @see org.springframework.batch.execution.aggregation.core.AggregationTimeoutPolicy
 * @see AggregationItemJmsMapper
 * @see JmsAggregationContextBuilder
 */
public class JmsAggregationService {

    private final Log logger = LogFactory.getLog(JmsAggregationService.class);

    /**
     * Waits for the specified number of items to be available.
     *
     * @param context the expected number of item(s)
     * @return the list of item(s)
     * @throws java.util.concurrent.TimeoutException if a timeout occurred
     * @see JmsAggregationContext
     */
    public <T> List<T> aggregate(JmsAggregationContext<T> context) throws TimeoutException {
        try {
            return doAggregate(context);
        } catch (JMSException e) {
            throw new ListenerExecutionFailedException("Failed to aggregate input message", e);
        }
    }

    protected <T> List<T> doAggregate(JmsAggregationContext<T> context) throws TimeoutException, JMSException {
        final long startTime = System.currentTimeMillis();
        final List<T> items = new ArrayList<T>();

        // Ordered list of listeners
        final List<AggregationItemListener<T>> orderedListeners = new OrderedComposite<AggregationItemListener<T>>(
                context.getAggregationItemListeners()).toList();


        final MessageConsumer consumer = context.getSession().createConsumer(context.getDestination());
        try {
            // TODO: handle global timeout?
            while (!context.getCompletionPolicy().shouldComplete()) {
                logger.debug("Waiting for message on [" + context.getDestination() + "]");
                final Message input = consumer.receive(context.getReceiveTimeout());
                if (input != null) {
                    try {
                        logger.debug("Input message received.");
                        final T item = context.getAggregationItemJmsMapper().map(input);
                        // Call back whatever the input is here
                        onItemRegistration(orderedListeners, item);
                        items.add(item);
                    } catch (JMSException e) {
                        throw new MessageConversionException("Failed to convert input message to item", e);
                    }
                } else if (context.getTimeoutPolicy().shouldTimeout(startTime)) {
                    // Check if we have have reached our timeout
                    final long delta = System.currentTimeMillis() - startTime;
                    throw new TimeoutException("Failed to retrieve expected item(s) " +
                            "after [" + delta + " ms]. Got [" + items.size() + "] item(s).");
                }
            }
            return items;
        } finally {
            JmsUtils.closeMessageConsumer(consumer);
        }
    }

    private <T> void onItemRegistration(List<AggregationItemListener<T>> listeners, T item) {
        for (AggregationItemListener<T> listener : listeners) {
            listener.onItemRegistration(item);
        }
    }

}
