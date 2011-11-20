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

import org.junit.Test;
import org.springframework.batch.execution.BaseExecutionJmsTest;
import org.springframework.batch.execution.aggregation.core.AggregationItemListener;
import org.springframework.batch.execution.aggregation.core.AggregationTimeoutPolicy;
import org.springframework.batch.execution.aggregation.core.support.CountBasedAggregationCompletionPolicy;
import org.springframework.batch.execution.aggregation.core.support.TimeBasedAggregationTimeoutPolicy;
import org.springframework.batch.execution.aggregation.jms.support.StringAggregationItemJmsMapper;
import org.springframework.batch.execution.support.jms.TextMessageCreator;
import org.springframework.jms.core.MessageCreator;
import org.springframework.jms.core.SessionCallback;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static junit.framework.Assert.*;

/**
 * @author Stephane Nicoll
 */
public class JmsAggregationServiceTests extends BaseExecutionJmsTest {

    private final JmsAggregationService instance = new JmsAggregationService();

    @Test
    public void sendOneMessageAndExpectOne() {
        executeTest(new SessionCallback<List<String>>() {
            public List<String> doInJms(Session session) throws JMSException {
                final Destination destination = sendMessagesOnTemporaryDestination(session, "Test1");

                final JmsAggregationContext<String> context = createContext(session, destination, 1,
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
        executeTest(new SessionCallback<List<String>>() {
            public List<String> doInJms(Session session) throws JMSException {
                final Destination destination = sendMessagesOnTemporaryDestination(session, "Test1");

                final AggregationItemTestListener listener = new AggregationItemTestListener(1);
                final JmsAggregationContext<String> context = createContext(session, destination, 1,
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
        executeTest(new SessionCallback<List<String>>() {
            public List<String> doInJms(Session session) throws JMSException {
                final Destination destination = sendMessagesOnTemporaryDestination(session, "Test1");

                final JmsAggregationContext<String> context = createContext(session, destination, 2,
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
    protected JmsAggregationContext<String> createContext(Session session, Destination destination,
                                                          int expectedMessageCount,
                                                          AggregationTimeoutPolicy timeoutPolicy,
                                                          AggregationItemListener<?>... aggregationItemListeners) {
        final JmsAggregationContextBuilder<String> builder = JmsAggregationContextBuilder
                .forDestination(String.class, session, destination)
                .withAggregationItemMapper(new StringAggregationItemJmsMapper())
                .withTimeoutPolicy(timeoutPolicy)
                .withCompletionPolicy(
                        new CountBasedAggregationCompletionPolicy(expectedMessageCount));
        for (AggregationItemListener aggregationItemListener : aggregationItemListeners) {
            builder.withAggregationItemListener(aggregationItemListener);
        }
        return builder.build();
    }


    protected Destination sendMessagesOnTemporaryDestination(Session session, String... content)
            throws JMSException {
        final List<MessageCreator> messageCreators = new ArrayList<MessageCreator>();
        for (String s : content) {
            messageCreators.add(new TextMessageCreator(s));
        }

        // Create a temp destination
        final Destination destination = session.createTemporaryQueue();

        // Sends the messages
        extendedJmsTemplate.send(messageCreators, session, destination);

        return destination;
    }

    protected <T> T executeTest(SessionCallback<T> callback) {
        return extendedJmsTemplate.execute(callback, true);
    }
}
