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
package org.springframework.batch.execution.jms.partition;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.batch.execution.jms.BaseJmsAwareTest;
import org.springframework.batch.execution.jms.partition.aggregation.AggregationTimeoutPolicy;
import org.springframework.batch.execution.jms.partition.aggregation.support.TimeBasedAggregationTimeoutPolicy;
import org.springframework.batch.execution.jms.partition.support.ExtendedJmsTemplate;
import org.springframework.batch.integration.partition.StepExecutionRequest;
import org.springframework.batch.integration.partition.StepExecutionResult;
import org.springframework.test.context.ContextConfiguration;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * @author Sebastien Gerard
 */
@ContextConfiguration
public class JmsStepExecutionRequestAggregatorTests extends BaseJmsAwareTest {

    public static final String STEP_NAME = "myStep";
    public static final String REQUEST_QUEUE = "queue/JmsStepExecutionRequestAggregatorTests";
    public static final Long JOB_EXECUTION_ID = 1234L;

    protected static final long FAKE_TIME_IN_MS = 2000;
    protected static final long DEFAULT_STEP_EXECUTION_TIMEOUT = 5;
    protected static final long DEFAULT_RECEIVE_TIMEOUT = 1;

    protected long currentStepExecutionId = 0;


    @Test
    public void launchNoRequest() throws TimeoutException {
        // Step should contribute at least every 2 min (120s). We check every 1 sec
        final TestContext testContext = createTestContext(120, 1);

        testContext.launchTest(0);
    }

    @Test
    public void waitTwoRequests() throws TimeoutException {
        final TestContext testContext = createDefaultTestContext();

        testContext.withRequests(createRandomRequest(), createRandomRequest());
        testContext.launchTest(2);
    }

    @Test
    public void noResult() {
        final TestContext testContext = createTestContext(2, 1);

        final StepExecutionRequest[] requests = {createRandomRequest(), createRandomRequest()};
        testContext.withRequests(requests);
        testContext.withNoResponsesFor(requests);

        try {
            testContext.launchTest(-1);
            Assert.fail("An exception should have been thrown");
        } catch (TimeoutException e) {
        }
    }

    @Test
    public void waitTwoRequestsOneRequestWithDelay() throws TimeoutException {
        final TestContext testContext = createDefaultTestContext();

        final StepExecutionRequest request = createRandomRequest();
        testContext.withRequests(request, createRandomRequest());
        testContext.withDelayFor(request);

        testContext.launchTest(2);
    }

    @Test
    public void oneRequestTooMuchTime() throws Exception {
        final TestContext testContext = createTestContext(1, 1);

        final StepExecutionRequest[] requests = {createRandomRequest(), createRandomRequest()};
        testContext.withRequests(requests);
        testContext.withDelayFor(requests[0]);

        try {
            testContext.launchTest(-1);
            Assert.fail("An exception should have been thrown");
        } catch (TimeoutException e) {
        }
    }

    protected TestContext createTestContext(long stepExecutionTimeout, long receiveTimeout) {
        return new TestContext(extendedJmsTemplate, stepExecutionTimeout, receiveTimeout);
    }

    protected TestContext createDefaultTestContext()  {
        return createTestContext(DEFAULT_STEP_EXECUTION_TIMEOUT, DEFAULT_RECEIVE_TIMEOUT);
    }

    protected StepExecutionRequest createRandomRequest() {
        return new StepExecutionRequest(STEP_NAME, JOB_EXECUTION_ID, currentStepExecutionId++);
    }

    protected static class TestContext {
        public final ExtendedJmsTemplate jmsTemplate;
        public final long stepExecutionTimeout;
        public final long receiveTimeout;
        public final TestableJmsStepExecutionRequestAggregator aggregator;

        public final Collection<StepExecutionRequest> requests = new HashSet<StepExecutionRequest>();
        public final Collection<StepExecutionRequest> noResponseForRequests = new HashSet<StepExecutionRequest>();
        public final Collection<StepExecutionRequest> delayForRequests = new HashSet<StepExecutionRequest>();

        public TestContext(ExtendedJmsTemplate jmsTemplate, long stepExecutionTimeout, long receiveTimeout) {
            this.jmsTemplate = jmsTemplate;
            this.stepExecutionTimeout = stepExecutionTimeout;
            this.receiveTimeout = receiveTimeout;

            aggregator = new TestableJmsStepExecutionRequestAggregator(this);
            aggregator.setJmsTemplate(jmsTemplate);
            aggregator.setReceiveTimeout(receiveTimeout);
            aggregator.setStepExecutionRequestQueueName(REQUEST_QUEUE);
            aggregator.setStepExecutionTimeout(stepExecutionTimeout);
        }

        public void withRequests(StepExecutionRequest... requests) {
            this.requests.addAll(Arrays.asList(requests));
        }

        public void withNoResponsesFor(StepExecutionRequest... requests) {
            final List<StepExecutionRequest> requestsAsList = Arrays.asList(requests);

            this.requests.addAll(requestsAsList);
            this.noResponseForRequests.addAll(requestsAsList);
        }

        public void withDelayFor(StepExecutionRequest... requests) {
            final List<StepExecutionRequest> requestsAsList = Arrays.asList(requests);

            this.requests.addAll(requestsAsList);
            this.delayForRequests.addAll(requestsAsList);
        }

        /**
         * Executes the test according to the current context. If the number of result
         * is <tt>-1</tt>, we expected an exception so no check on the size of the
         * result should be made.
         *
         * @param numberResult the number of result expected or -1 to ignore that check
         * @return the list of results
         * @throws TimeoutException if a timeout occurs
         */
        public List<StepExecutionResult> launchTest(Integer numberResult) throws TimeoutException {
            final List<StepExecutionResult> stepExecutionResultList =
                    aggregator.aggregate(new ArrayList<StepExecutionRequest>(requests));

            if (numberResult != -1) {
                Assert.assertEquals("The number of results is not the expected one",
                        (int) numberResult, stepExecutionResultList.size());
            }

            return stepExecutionResultList;
        }
    }

    protected static class TestableJmsStepExecutionRequestAggregator extends JmsStepExecutionRequestAggregator {
        private final TestContext testDescription;

        public TestableJmsStepExecutionRequestAggregator(TestContext testDescription) {
            this.testDescription = testDescription;
        }

        @Override
        protected AggregationTimeoutPolicy createTimeoutPolicy(List<StepExecutionRequest> requests) {
            return new TimeBasedAggregationTimeoutPolicy(testDescription.stepExecutionTimeout * 1000);
        }

        @Override
        @SuppressWarnings({"SuspiciousMethodCalls"})
        protected void enrichMessage(ObjectMessage message) {
            try {
                final boolean waitResponse = !testDescription.noResponseForRequests.contains(message.getObject());
                if (waitResponse) {
                    message.setStringProperty(StepExecutionRequestTestListener.SEND_REPLY_KEY, "DummyReply ");
                }

                final boolean delay = testDescription.delayForRequests.contains(message.getObject());
                if (delay) {
                    message.setLongProperty(StepExecutionRequestTestListener.FAKE_PROCESSING_TIME_KEY, FAKE_TIME_IN_MS);
                }
            } catch (JMSException e) {
                throw new IllegalStateException("Error while setting the JMS property", e);
            }
        }
    }

}
