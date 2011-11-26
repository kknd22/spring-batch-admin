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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.ChannelCallback;
import org.springframework.amqp.support.converter.SerializerMessageConverter;
import org.springframework.batch.execution.BaseExecutionAmqpTest;
import org.springframework.batch.execution.aggregation.amqp.support.StringAggregationItemAmqpMapper;
import org.springframework.batch.execution.aggregation.core.AggregationItemListener;
import org.springframework.batch.execution.aggregation.core.AggregationTimeoutPolicy;
import org.springframework.batch.execution.aggregation.core.support.CountBasedAggregationCompletionPolicy;
import org.springframework.batch.execution.aggregation.core.support.TimeBasedAggregationTimeoutPolicy;
import org.springframework.batch.execution.aggregation.test.AggregationItemTestListener;

import javax.jms.JMSException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static junit.framework.Assert.*;

/**
 * @author Stephane Nicoll
 */
public class AmqpAggregationServiceTests extends BaseExecutionAmqpTest {

    private final Log logger = LogFactory.getLog(AmqpAggregationServiceTests.class);

    private final SerializerMessageConverter messageConverter = new SerializerMessageConverter();
    private final AmqpAggregationService instance = new AmqpAggregationService();

    @Test
    public void sendOneMessageAndExpectOne() {
        executeTest(new ChannelCallback<List<String>>() {
            public List<String> doInRabbit(Channel channel) throws Exception {
                final String destination = sendMessagesOnTemporaryDestination(channel, "Test1");

                final AmqpAggregationContext<String> context = createContext(channel, destination, 1,
                        new TimeBasedAggregationTimeoutPolicy(200L));

                try {
                    final List<String> result = instance.aggregate(context);
                    assertResult(result, "Test1");
                    return result;
                } catch (TimeoutException e) {
                    throw new IllegalStateException("Timeout was not expected", e);
                }
            }
        });
    }

    @Test
    public void sendOneMessageCallTheRegistrationListenerOnce() {
        executeTest(new ChannelCallback<List<String>>() {
            public List<String> doInRabbit(Channel channel) throws Exception {
                final String destination = sendMessagesOnTemporaryDestination(channel, "Test1");

                final AggregationItemTestListener listener = new AggregationItemTestListener(1);
                final AmqpAggregationContext<String> context = createContext(channel, destination, 1,
                        new TimeBasedAggregationTimeoutPolicy(200L), listener);

                try {
                    final List<String> result = instance.aggregate(context);
                    assertResult(result, "Test1");
                    // Make sure the listener was called
                    listener.validate();
                    return result;
                } catch (TimeoutException e) {
                    throw new IllegalStateException("Timeout was not expected", e);
                }
            }
        });
    }

    @Test
    public void sendOneMessageAndExpectTwo() {
        executeTest(new ChannelCallback<List<String>>() {
            public List<String> doInRabbit(Channel channel) throws Exception {
                final String destination = sendMessagesOnTemporaryDestination(channel, "Test1");

                final AmqpAggregationContext<String> context = createContext(channel, destination, 2,
                        new TimeBasedAggregationTimeoutPolicy(200L));

                try {
                    final List<String> result = instance.aggregate(context);
                    fail("Should have gone in timeout but got " + result);
                } catch (TimeoutException e) {
                    // OK
                }
                return null;
            }
        });
    }


    protected void assertResult(List<String> actual, String... expectedItems) {
        assertNotNull("result could not be null", actual);
        for (String expectedItem : expectedItems) {
            assertTrue("Item [" + expectedItem + "] not found in " + actual, actual.contains(expectedItem));
        }
        assertEquals("Wrong number of expected items. Got " + actual, expectedItems.length, actual.size());

    }

    @SuppressWarnings({"unchecked"})
    protected AmqpAggregationContext<String> createContext(Channel channel, String destination,
                                                           int expectedMessageCount,
                                                           AggregationTimeoutPolicy timeoutPolicy,
                                                           AggregationItemListener<?>... aggregationItemListeners) {
        final AmqpAggregationContextBuilder<String> builder = AmqpAggregationContextBuilder
                .forDestination(String.class, channel, destination)
                .withAggregationItemMapper(new StringAggregationItemAmqpMapper())
                .withTimeoutPolicy(timeoutPolicy)
                .withCompletionPolicy(
                        new CountBasedAggregationCompletionPolicy(expectedMessageCount));
        for (AggregationItemListener aggregationItemListener : aggregationItemListeners) {
            builder.withAggregationItemListener(aggregationItemListener);
        }
        return builder.build();
    }


    protected String sendMessagesOnTemporaryDestination(Channel channel, String... content)
            throws JMSException, IOException {

        // Create a temp destination
        final String destination = createTemporaryQueue(channel);

        final List<Message> amqpMessages = new ArrayList<Message>();
        for (String message : content) {
            final Message m = toMessage(message);
            amqpMessages.add(m);
        }
        logger.debug("Sending [" + amqpMessages.size() + "] message(s).");
        extendedAmqpTemplate.send(amqpMessages, channel, null, destination);

        return destination;
    }

    protected <T> T executeTest(ChannelCallback<T> callback) {
        return extendedAmqpTemplate.execute(callback);
    }


    /**
     * Creates a temporary queue for the specified {@link Channel}.
     *
     * @param channel the channel to use
     * @return the name of a temporary queue
     * @throws IOException if the queue could not be created
     */
    private String createTemporaryQueue(final Channel channel) throws IOException {
        AMQP.Queue.DeclareOk queueDeclaration = channel.queueDeclare();
        return queueDeclaration.getQueue();
    }

    private Message toMessage(String input) {
        return messageConverter.toMessage(input, new MessageProperties());
    }


}
