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
package org.springframework.batch.integration.partition.amqp;

import org.junit.runner.RunWith;
import org.springframework.amqp.core.Message;
import org.springframework.batch.execution.aggregation.core.AggregationTimeoutPolicy;
import org.springframework.batch.execution.aggregation.core.support.TimeBasedAggregationTimeoutPolicy;
import org.springframework.batch.execution.support.amqp.ExtendedRabbitTemplate;
import org.springframework.batch.integration.partition.AbstractStepExecutionRequestAggregatorTests;
import org.springframework.batch.integration.partition.StepExecutionRequest;
import org.springframework.batch.integration.partition.StepExecutionRequestAggregator;
import org.springframework.batch.integration.partition.jms.StepExecutionRequestTestListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;

/**
 * @author Stephane Nicoll
 */
@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext
public class AmqpStepExecutionRequestAggregatorTests extends
        AbstractStepExecutionRequestAggregatorTests<AmqpStepExecutionRequestAggregatorTests.AmqpTestContext> {

    public static final String REQUEST_QUEUE = "queue.StepExecutionRequestAggregatorTests";

    @Autowired
    protected ExtendedRabbitTemplate amqpTemplate;

    protected AmqpTestContext createTestContext(long stepExecutionTimeout, long receiveTimeout) {
        return new AmqpTestContext(amqpTemplate, stepExecutionTimeout, receiveTimeout);
    }

    public static class AmqpTestContext extends AbstractStepExecutionRequestAggregatorTests.TestContext {
        public final TestableAmqpStepExecutionRequestAggregator aggregator;

        public AmqpTestContext(ExtendedRabbitTemplate amqpTemplate, long stepExecutionTimeout, long receiveTimeout) {
            super(stepExecutionTimeout, receiveTimeout);

            aggregator = new TestableAmqpStepExecutionRequestAggregator(this);
            aggregator.setAmqpTemplate(amqpTemplate);
            aggregator.setReceiveTimeout(receiveTimeout);
            aggregator.setStepExecutionRequestQueueName(REQUEST_QUEUE);
            aggregator.setStepExecutionTimeout(stepExecutionTimeout);
        }

        @Override
        protected StepExecutionRequestAggregator getStepExecutionRequestAggregator() {
            return aggregator;
        }
    }

    protected static class TestableAmqpStepExecutionRequestAggregator extends AmqpStepExecutionRequestAggregator {
        private final AmqpTestContext testDescription;

        public TestableAmqpStepExecutionRequestAggregator(AmqpTestContext testDescription) {
            this.testDescription = testDescription;
        }

        @Override
        protected AggregationTimeoutPolicy createTimeoutPolicy(List<StepExecutionRequest> requests) {
            return new TimeBasedAggregationTimeoutPolicy(testDescription.stepExecutionTimeout * 1000);
        }

        @Override
        protected Message enrichMessage(Message message) {
            final StepExecutionRequest stepExecutionRequest = (StepExecutionRequest) getMessageConverter().fromMessage(message);

            final boolean waitResponse = !testDescription.noResponseForRequests.contains(stepExecutionRequest);
            if (waitResponse) {
                message.getMessageProperties().setHeader(
                        StepExecutionRequestTestListener.SEND_REPLY_KEY, "DummyReply ");
            }

            final boolean delay = testDescription.delayForRequests.contains(stepExecutionRequest);
            if (delay) {
                message.getMessageProperties().setHeader(
                        StepExecutionRequestTestListener.FAKE_PROCESSING_TIME_KEY, FAKE_TIME_IN_MS);
            }
            return message;

        }
    }
}
