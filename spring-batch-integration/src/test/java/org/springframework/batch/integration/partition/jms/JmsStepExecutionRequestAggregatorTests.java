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
package org.springframework.batch.integration.partition.jms;

import org.junit.runner.RunWith;
import org.springframework.batch.execution.aggregation.core.AggregationTimeoutPolicy;
import org.springframework.batch.execution.aggregation.core.support.TimeBasedAggregationTimeoutPolicy;
import org.springframework.batch.execution.support.jms.ExtendedJmsTemplate;
import org.springframework.batch.integration.partition.AbstractStepExecutionRequestAggregatorTests;
import org.springframework.batch.integration.partition.StepExecutionRequest;
import org.springframework.batch.integration.partition.StepExecutionRequestAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;
import java.util.List;

/**
 * @author Sebastien Gerard
 */
@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext
public class JmsStepExecutionRequestAggregatorTests extends
        AbstractStepExecutionRequestAggregatorTests<JmsStepExecutionRequestAggregatorTests.JmsTestContext> {

    public static final String REQUEST_QUEUE = "queue/JmsStepExecutionRequestAggregatorTests";

    @Autowired
    protected ExtendedJmsTemplate jmsTemplate;

    protected JmsTestContext createTestContext(long stepExecutionTimeout, long receiveTimeout) {
        return new JmsTestContext(jmsTemplate, stepExecutionTimeout, receiveTimeout);
    }

    public static class JmsTestContext extends AbstractStepExecutionRequestAggregatorTests.TestContext {
        public final TestableJmsStepExecutionRequestAggregator aggregator;

        public JmsTestContext(ExtendedJmsTemplate jmsTemplate, long stepExecutionTimeout, long receiveTimeout) {
            super(stepExecutionTimeout, receiveTimeout);

            aggregator = new TestableJmsStepExecutionRequestAggregator(this);
            aggregator.setJmsTemplate(jmsTemplate);
            aggregator.setReceiveTimeout(receiveTimeout);
            aggregator.setStepExecutionRequestQueueName(REQUEST_QUEUE);
            aggregator.setStepExecutionTimeout(stepExecutionTimeout);
        }

        @Override
        protected StepExecutionRequestAggregator getStepExecutionRequestAggregator() {
            return aggregator;
        }
    }

    protected static class TestableJmsStepExecutionRequestAggregator extends JmsStepExecutionRequestAggregator {
        private final JmsTestContext testDescription;

        public TestableJmsStepExecutionRequestAggregator(JmsTestContext testDescription) {
            this.testDescription = testDescription;
        }

        @Override
        protected AggregationTimeoutPolicy createTimeoutPolicy(List<StepExecutionRequest> requests) {
            return new TimeBasedAggregationTimeoutPolicy(testDescription.stepExecutionTimeout * 1000);
        }

        @Override
        protected ObjectMessage enrichMessage(ObjectMessage message) {
            try {
                final StepExecutionRequest stepExecutionRequest = (StepExecutionRequest) message.getObject();
                final boolean waitResponse = !testDescription.noResponseForRequests.contains(stepExecutionRequest);
                if (waitResponse) {
                    message.setStringProperty(StepExecutionRequestTestListener.SEND_REPLY_KEY, "DummyReply ");
                }

                final boolean delay = testDescription.delayForRequests.contains(stepExecutionRequest);
                if (delay) {
                    message.setLongProperty(StepExecutionRequestTestListener.FAKE_PROCESSING_TIME_KEY, FAKE_TIME_IN_MS);
                }
                return message;
            } catch (JMSException e) {
                throw new IllegalStateException("Error while setting the JMS property", e);
            }
        }
    }

}
