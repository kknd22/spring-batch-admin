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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.RabbitUtils;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.MessagePropertiesConverter;
import org.springframework.batch.core.listener.OrderedComposite;
import org.springframework.batch.execution.aggregation.core.AggregationItemListener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A message aggregator, based on a context which basically waits for incoming items
 * on a given destination. Uses different policy to determine if the aggregation is
 * done or should go in timeout.
 *
 * @author Stephane Nicoll
 * @see org.springframework.batch.execution.aggregation.core.AggregationCompletionPolicy
 * @see org.springframework.batch.execution.aggregation.core.AggregationTimeoutPolicy
 * @see org.springframework.batch.execution.aggregation.core.AggregationItemMapper
 * @see AmqpAggregationContextBuilder
 */
public class AmqpAggregationService {

    private final Log logger = LogFactory.getLog(AmqpAggregationService.class);

    /**
     * Waits for the specified number of items to be available.
     *
     * @param context the expected number of item(s)
     * @return the list of item(s)
     * @throws java.util.concurrent.TimeoutException if a timeout occurred
     * @see AmqpAggregationContext
     */
    public <T> List<T> aggregate(AmqpAggregationContext<T> context) throws TimeoutException {
        try {
            return doAggregate(context);
        } catch (IOException e) {
            throw RabbitUtils.convertRabbitAccessException(e);
        }
    }

    protected <T> List<T> doAggregate(AmqpAggregationContext<T> context) throws TimeoutException, IOException {
        final long startTime = System.currentTimeMillis();
        final List<T> items = new ArrayList<T>();

        // Ordered list of listeners
        final List<AggregationItemListener<T>> orderedListeners = new OrderedComposite<AggregationItemListener<T>>(
                context.getAggregationItemListeners()).toList();


        final AggregationItemConsumer consumer = initializeAndStartConsumer(context);
        try {
            // TODO: handle global timeout?
            while (!context.getCompletionPolicy().shouldComplete()) {
                logger.debug("Waiting for message on [" + context.getDestination() + "]");
                final Message input = receive(consumer, context.getReceiveTimeout());
                if (input != null) {
                    logger.debug("Input message received.");
                    final T item = context.getAggregationItemMapper().map(input);
                    // Call back whatever the input is here
                    onItemRegistration(orderedListeners, item);
                    items.add(item);
                } else if (context.getTimeoutPolicy().shouldTimeout(startTime)) {
                    // Check if we have have reached our timeout
                    final long delta = System.currentTimeMillis() - startTime;
                    throw new TimeoutException("Failed to retrieve expected item(s) " +
                            "after [" + delta + " ms]. Got [" + items.size() + "] item(s).");
                }
            }
            return items;
        } finally {
            consumer.stop();
        }
    }

    private <T> void onItemRegistration(List<AggregationItemListener<T>> listeners, T item) {
        for (AggregationItemListener<T> listener : listeners) {
            listener.onItemRegistration(item);
        }
    }

    private Message receive(AggregationItemConsumer consumer, long replyTimeout) {
        try {
            return (replyTimeout <= 0) ? consumer.getReplies().take()
                    : consumer.getReplies().poll(replyTimeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return null;
    }

    /**
     * Initializes and start a consumer to collect replies.
     *
     * @param context the context to use
     * @return a started consumer
     * @throws IOException if the consumer could not start properly
     */
    private AggregationItemConsumer initializeAndStartConsumer(AmqpAggregationContext<?> context)
            throws IOException {
        final String consumerTag = UUID.randomUUID().toString();
        final AggregationItemConsumer consumer =
                new AggregationItemConsumer(context.getChannel(), context.getEncoding(), consumerTag);

        boolean noAck = false;
        boolean noLocal = true;
        boolean exclusive = true;
        if (logger.isDebugEnabled()) {
            logger.debug("Starting aggregation item consumer with tag [" + consumerTag + "]");
        }
        consumer.getChannel().basicConsume(context.getDestination(), noAck,
                consumerTag, noLocal, exclusive, null, consumer);
        return consumer;
    }

    /**
     * A consumer that will add the aggregation replies in a queue.
     */
    private class AggregationItemConsumer extends DefaultConsumer {

        private final Log logger = LogFactory.getLog(AggregationItemConsumer.class);


        private final String encoding;
        private final String consumerTag;
        private final BlockingQueue<Message> replies = new LinkedBlockingQueue<Message>();
        private volatile MessagePropertiesConverter messagePropertiesConverter =
                new DefaultMessagePropertiesConverter();

        /**
         * Creates a consumer.
         *
         * @param channel the channel to use
         * @param encoding the encoding to use
         * @param consumerTag the related consumer tag
         */
        private AggregationItemConsumer(Channel channel, String encoding, String consumerTag) {
            super(channel);
            this.encoding = encoding;
            this.consumerTag = consumerTag;
        }

        @Override
        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                   byte[] body) throws IOException {
            final MessageProperties messageProperties = messagePropertiesConverter.toMessageProperties(
                    properties, envelope, encoding);
            final Message reply = new Message(body, messageProperties);

            replies.add(reply);
        }

        /**
         * The {@link BlockingQueue} holding the replies.
         *
         * @return the queue containing the replies
         */
        public BlockingQueue<Message> getReplies() {
            return replies;
        }

        /**
         * Stops listening for incoming messages.
         *
         * @throws IOException if stopping failed
         */
        public void stop() throws IOException {
            if (logger.isDebugEnabled()) {
                logger.debug("Stopping aggregation item consumer with tag [" + consumerTag + "]");
            }
            getChannel().basicCancel(consumerTag);

        }
    }
}
