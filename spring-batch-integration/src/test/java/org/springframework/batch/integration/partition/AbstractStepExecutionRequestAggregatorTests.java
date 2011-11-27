package org.springframework.batch.integration.partition;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * Shared tests for {@link StepExecutionRequestAggregator}.
 *
 * @author Stephane Nicoll
 */
public abstract class AbstractStepExecutionRequestAggregatorTests<C extends AbstractStepExecutionRequestAggregatorTests.TestContext> {

    public static final String STEP_NAME = "myStep";
    public static final Long JOB_EXECUTION_ID = 1234L;

    protected static final long DEFAULT_STEP_EXECUTION_TIMEOUT = 5;
    protected static final long DEFAULT_RECEIVE_TIMEOUT = 1;
    protected static final long FAKE_TIME_IN_MS = 2000;

    protected long currentStepExecutionId = 0;

    @Test
    public void launchNoRequest() throws TimeoutException {
        // Step should contribute at least every 2 min (120s). We check every 1 sec
        final C testContext = createTestContext(120, 1);

        testContext.launchTest(0);
    }

    @Test
    public void waitTwoRequests() throws TimeoutException {
        final C testContext = createDefaultTestContext();

        testContext.withRequests(createRandomRequest(), createRandomRequest());
        testContext.launchTest(2);
    }

    @Test
    public void noResult() {
        final C testContext = createTestContext(2, 1);

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
        final C testContext = createDefaultTestContext();

        final StepExecutionRequest request = createRandomRequest();
        testContext.withRequests(request, createRandomRequest());
        testContext.withDelayFor(request);

        testContext.launchTest(2);
    }

    @Test
    public void oneRequestTooMuchTime() throws Exception {
        final C testContext = createTestContext(1, 1);

        final StepExecutionRequest[] requests = {createRandomRequest(), createRandomRequest()};
        testContext.withRequests(requests);
        testContext.withDelayFor(requests[0]);

        try {
            testContext.launchTest(-1);
            Assert.fail("An exception should have been thrown");
        } catch (TimeoutException e) {
        }
    }


    protected abstract C createTestContext(long stepExecutionTimeout, long receiveTimeout);

    protected C createDefaultTestContext() {
        return createTestContext(DEFAULT_STEP_EXECUTION_TIMEOUT, DEFAULT_RECEIVE_TIMEOUT);
    }

    protected StepExecutionRequest createRandomRequest() {
        return new StepExecutionRequest(STEP_NAME, JOB_EXECUTION_ID, currentStepExecutionId++);
    }

    public static abstract class TestContext {
        public final long stepExecutionTimeout;
        public final long receiveTimeout;

        public final Collection<StepExecutionRequest> requests = new HashSet<StepExecutionRequest>();
        public final Collection<StepExecutionRequest> noResponseForRequests = new HashSet<StepExecutionRequest>();
        public final Collection<StepExecutionRequest> delayForRequests = new HashSet<StepExecutionRequest>();

        public TestContext(long stepExecutionTimeout, long receiveTimeout) {
            this.stepExecutionTimeout = stepExecutionTimeout;
            this.receiveTimeout = receiveTimeout;
        }

        protected abstract StepExecutionRequestAggregator getStepExecutionRequestAggregator();

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
         * @throws java.util.concurrent.TimeoutException if a timeout occurs
         */
        public List<StepExecutionResult> launchTest(Integer numberResult) throws TimeoutException {
            final List<StepExecutionResult> stepExecutionResultList =
                    getStepExecutionRequestAggregator().aggregate(new ArrayList<StepExecutionRequest>(requests));

            if (numberResult != -1) {
                Assert.assertEquals("The number of results is not the expected one",
                        (int) numberResult, stepExecutionResultList.size());
            }

            return stepExecutionResultList;
        }
    }
}
