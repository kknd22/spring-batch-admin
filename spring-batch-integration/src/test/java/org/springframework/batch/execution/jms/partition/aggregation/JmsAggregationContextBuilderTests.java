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

import org.junit.Test;
import org.springframework.batch.execution.jms.partition.aggregation.support.CountBasedAggregationCompletionPolicy;
import org.springframework.batch.execution.jms.partition.aggregation.support.StringAggregationItemJmsMapper;
import org.springframework.batch.execution.jms.partition.aggregation.support.TimeBasedAggregationTimeoutPolicy;
import org.springframework.jms.core.SessionCallback;

import javax.jms.JMSException;
import javax.jms.Session;

import static junit.framework.Assert.*;

/**
 * @author Stephane Nicoll
 */
public class JmsAggregationContextBuilderTests extends BaseJmsAggregationTest {

    @Test
    public void builderWithNullSession() {
        try {
            JmsAggregationContextBuilder.forDestination(String.class, null, queueA);
            fail("Failed to create builder with a null session.");
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    @Test
    public void builderWithNullDestination() {
        extendedJmsTemplate.execute(new SessionCallback<Object>() {
            public Object doInJms(Session session) throws JMSException {
                try {
                    JmsAggregationContextBuilder.forDestination(String.class, session, null);
                    fail("Failed to create builder with a null destination.");
                } catch (IllegalArgumentException e) {
                    // OK
                }
                return null;
            }
        });
    }

    @Test
    public void builderWithNullCompletionPolicy() {
        extendedJmsTemplate.execute(new SessionCallback<Object>() {
            public Object doInJms(Session session) throws JMSException {
                try {
                    JmsAggregationContextBuilder
                            .forDestination(String.class, session, queueA)
                            .build();
                    fail("Failed to create builder with a null completion policy.");
                } catch (IllegalArgumentException e) {
                    // OK
                }
                return null;
            }
        });
    }

    @Test
    public void builderWithNullTimeoutPolicy() {
        extendedJmsTemplate.execute(new SessionCallback<Object>() {
            public Object doInJms(Session session) throws JMSException {
                try {
                    JmsAggregationContextBuilder
                            .forDestination(String.class, session, queueA)
                            .withCompletionPolicy(new CountBasedAggregationCompletionPolicy(1))
                            .build();
                    fail("Failed to create builder with a null timeout policy.");
                } catch (IllegalArgumentException e) {
                    // OK
                }
                return null;
            }
        });
    }

    @Test
    public void builderWithNullAggregationItemMapper() {
        extendedJmsTemplate.execute(new SessionCallback<Object>() {
            public Object doInJms(Session session) throws JMSException {
                try {
                    JmsAggregationContextBuilder
                            .forDestination(String.class, session, queueA)
                            .withCompletionPolicy(new CountBasedAggregationCompletionPolicy(1))
                            .withTimeoutPolicy(new TimeBasedAggregationTimeoutPolicy(1000))
                            .build();
                    fail("Failed to create builder with a null aggregation item mapper.");
                } catch (IllegalArgumentException e) {
                    // OK
                }
                return null;
            }
        });
    }

    @Test
    public void simpleBuilder() {
        initializeDefaultBuilder().build();
    }

    @Test
    public void builderWithReceiveTimeout() {
        final JmsAggregationContext<String> context = initializeDefaultBuilder().withReceiveTimeout(100).build();
        assertEquals(100L, context.getReceiveTimeout());
    }

    @Test
    public void ensureCompletionPolicyIsRegisteredAsAListener() {
        final AggregationCompletionPolicy policy = new CountBasedAggregationCompletionPolicy(1);
        final JmsAggregationContext<String> context = initializeDefaultBuilder().withCompletionPolicy(policy).build();
        assertTrue("Completion policy should have been registered as a listener",
                context.getAggregationItemListeners().contains(policy));
    }

    @Test
    public void ensureTimeoutPolicyIsRegisteredAsAListenerIfApplicable() {
        final AggregationTimeoutPolicy policy = new ExtendedAggregationTimeoutPolity();
        final JmsAggregationContext<String> context = initializeDefaultBuilder().withTimeoutPolicy(policy).build();
        assertTrue("Timeout policy should have been registered as a listener if it implements the listener interface",
                context.getAggregationItemListeners().contains(policy));
    }

    @Test
    public void builderWithNegativeReceiveTimeout() {
        try {
            initializeDefaultBuilder().withReceiveTimeout(-5);
            fail("Failed to set a negative receive timeout");
        } catch (IllegalStateException e) {
            // OK
        }

    }

    protected JmsAggregationContextBuilder<String> initializeDefaultBuilder() {
        return extendedJmsTemplate.execute(new SessionCallback<JmsAggregationContextBuilder<String>>() {
            public JmsAggregationContextBuilder<String> doInJms(Session session) throws JMSException {
                return JmsAggregationContextBuilder
                        .forDestination(String.class, session, queueA)
                        .withCompletionPolicy(new CountBasedAggregationCompletionPolicy(1))
                        .withTimeoutPolicy(new TimeBasedAggregationTimeoutPolicy(1000))
                        .withAggregationItemMapper(new StringAggregationItemJmsMapper());

            }

        });
    }

    /**
     * A test {@link AggregationTimeoutPolicy} that also implements the
     * {@link AggregationItemListener} interface.
     */
    private static class ExtendedAggregationTimeoutPolity
            implements AggregationTimeoutPolicy, AggregationItemListener {

        public void onItemRegistration(Object item) {

        }

        public boolean shouldTimeout(long startTime) {
            return false;
        }
    }
}