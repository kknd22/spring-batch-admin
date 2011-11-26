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
import org.junit.Test;
import org.springframework.amqp.rabbit.core.ChannelCallback;
import org.springframework.batch.execution.BaseExecutionAmqpTest;
import org.springframework.batch.execution.aggregation.amqp.support.StringAggregationItemAmqpMapper;
import org.springframework.batch.execution.aggregation.core.AggregationCompletionPolicy;
import org.springframework.batch.execution.aggregation.core.support.CountBasedAggregationCompletionPolicy;
import org.springframework.batch.execution.aggregation.core.support.TimeBasedAggregationTimeoutPolicy;
import org.springframework.batch.execution.aggregation.test.ExtendedAggregationTimeoutPolity;

import static junit.framework.Assert.*;

/**
 * @author Stephane Nicoll
 */
public class AmqpAggregationContextBuilderTests extends BaseExecutionAmqpTest {

    private final String replyQueue = "replyQueue";


    @Test(expected = IllegalArgumentException.class)
    public void builderWithNullSession() {
        AmqpAggregationContextBuilder.forDestination(String.class, null, replyQueue);
    }

    @Test
    public void builderWithNullDestination() {
        extendedAmqpTemplate.execute(new ChannelCallback<Object>() {
            public Object doInRabbit(Channel channel) throws Exception {
                try {
                    AmqpAggregationContextBuilder.forDestination(String.class, channel, null);
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
        extendedAmqpTemplate.execute(new ChannelCallback<Object>() {
            public Object doInRabbit(Channel channel) throws Exception {
                try {
                    AmqpAggregationContextBuilder
                            .forDestination(String.class, channel, replyQueue)
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
        extendedAmqpTemplate.execute(new ChannelCallback<Object>() {
            public Object doInRabbit(Channel channel) throws Exception {
                try {
                    AmqpAggregationContextBuilder
                            .forDestination(String.class, channel, replyQueue)
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
        extendedAmqpTemplate.execute(new ChannelCallback<Object>() {
            public Object doInRabbit(Channel channel) throws Exception {
                try {
                    AmqpAggregationContextBuilder
                            .forDestination(String.class, channel, replyQueue)
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
        final AmqpAggregationContext<String> context = initializeDefaultBuilder().withReceiveTimeout(100).build();
        assertEquals(100L, context.getReceiveTimeout());
    }

    @Test
    public void builderWithEncoding() {
        final AmqpAggregationContext<String> context = initializeDefaultBuilder().withEncoding("ISO-8859-1").build();
        assertEquals("ISO-8859-1", context.getEncoding());
    }

    @Test(expected = IllegalArgumentException.class)
    public void builderWithNullEncoding() {
        initializeDefaultBuilder().withEncoding(null);
    }

    @Test
    public void ensureCompletionPolicyIsRegisteredAsAListener() {
        final AggregationCompletionPolicy policy = new CountBasedAggregationCompletionPolicy(1);
        final AmqpAggregationContext<String> context = initializeDefaultBuilder().withCompletionPolicy(policy).build();
        assertTrue("Completion policy should have been registered as a listener",
                context.getAggregationItemListeners().contains(policy));
    }

    @Test
    public void ensureTimeoutPolicyIsRegisteredAsAListenerIfApplicable() {
        final ExtendedAggregationTimeoutPolity policy = new ExtendedAggregationTimeoutPolity();
        final AmqpAggregationContext<String> context = initializeDefaultBuilder().withTimeoutPolicy(policy).build();
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

    protected AmqpAggregationContextBuilder<String> initializeDefaultBuilder() {
        return extendedAmqpTemplate.execute(new ChannelCallback<AmqpAggregationContextBuilder<String>>() {
            public AmqpAggregationContextBuilder<String> doInRabbit(Channel channel) throws Exception {
                return AmqpAggregationContextBuilder
                        .forDestination(String.class, channel, replyQueue)
                        .withCompletionPolicy(new CountBasedAggregationCompletionPolicy(1))
                        .withTimeoutPolicy(new TimeBasedAggregationTimeoutPolicy(1000))
                        .withAggregationItemMapper(new StringAggregationItemAmqpMapper());

            }
        });
    }

}
