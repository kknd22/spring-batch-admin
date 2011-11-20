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
package org.springframework.batch.integration.partition.support;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.support.SimpleJobExplorer;
import org.springframework.batch.core.repository.dao.MapExecutionContextDao;
import org.springframework.batch.core.repository.dao.MapJobExecutionDao;
import org.springframework.batch.core.repository.dao.MapJobInstanceDao;
import org.springframework.batch.core.repository.dao.MapStepExecutionDao;
import org.springframework.batch.integration.partition.StepExecutionRequest;
import org.springframework.batch.integration.partition.StepExecutionResult;
import org.springframework.batch.integration.partition.support.StepExecutionAggregationTimeoutPolicy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Sebastien Gerard
 */
public class StepExecutionAggregationTimeoutPolicyTests {

    protected static final long JOB_EXECUTION_ID = 1234L;
    protected static final String STEP_NAME = "myStep";
    protected static final long PROCESSING_TIMEOUT_TWENTY_MINUTES = 20 * 60 * 1000;
    protected static final String NO_TIMEOUT_EXPECTED = "No timeout expected.";
    protected static final String TIMEOUT_EXPECTED = "Timeout expected.";

    private long currentStepExecutionId = 0;


    @Test
    public void noRequest() {
        final long startTime = minutesAgo(5);
        Assert.assertFalse(NO_TIMEOUT_EXPECTED,
                createPolicy(PROCESSING_TIMEOUT_TWENTY_MINUTES).shouldTimeout(startTime));
    }

    @Test
    public void updateDateInFuture() {
        // that may happen if clocks are not synchronized
        final long startTime = minutesAgo(5);
        Assert.assertFalse(NO_TIMEOUT_EXPECTED,
                createPolicy(PROCESSING_TIMEOUT_TWENTY_MINUTES,
                        createStepExecution(startTime, minutesAgo(-1), BatchStatus.STARTED))
                        .shouldTimeout(startTime));
    }

    @Test
    public void commonUseCaseNoTimeout() {
        final long startTime = minutesAgo(21);
        Assert.assertFalse(NO_TIMEOUT_EXPECTED,
                createPolicy(PROCESSING_TIMEOUT_TWENTY_MINUTES,
                        createStepExecution(startTime, minutesAgo(7), BatchStatus.COMPLETED),
                        createStepExecution(startTime, minutesAgo(10), BatchStatus.STARTING),
                        createStepExecution(startTime, minutesAgo(19), BatchStatus.STARTED))
                        .shouldTimeout(startTime));
    }

    @Test
    public void atLeastOneAlive() {
        final long startTime = minutesAgo(23);
        Assert.assertFalse(NO_TIMEOUT_EXPECTED,
                createPolicy(PROCESSING_TIMEOUT_TWENTY_MINUTES,
                        createStepExecution(startTime, minutesAgo(1), BatchStatus.STARTED),
                        createStepExecution(startTime, minutesAgo(12), BatchStatus.STARTING),
                        createStepExecution(startTime, minutesAgo(21), BatchStatus.STARTED))
                        .shouldTimeout(startTime));
    }

    @Test
    public void noOneAlive() {
        final long startTime = minutesAgo(23);
        Assert.assertTrue(TIMEOUT_EXPECTED,
                createPolicy(PROCESSING_TIMEOUT_TWENTY_MINUTES,
                        createStepExecution(startTime, minutesAgo(21), BatchStatus.STARTED),
                        createStepExecution(startTime, minutesAgo(22), BatchStatus.STARTING),
                        createStepExecution(startTime, minutesAgo(21), BatchStatus.STARTED))
                        .shouldTimeout(startTime));
    }

    @Test
    public void wholeProcessingBiggerThanTimeout() {
        final long startTime = minutesAgo(24);
        Assert.assertFalse(NO_TIMEOUT_EXPECTED,
                createPolicy(PROCESSING_TIMEOUT_TWENTY_MINUTES,
                        createStepExecution(startTime, minutesAgo(19), BatchStatus.STARTED))
                        .shouldTimeout(startTime));
    }

    @Test
    public void wholeProcessingFinishedAllDead() {
        // if the step result has not been received, the step is still monitored whatever its status
        final long startTime = minutesAgo(24);
        Assert.assertTrue(TIMEOUT_EXPECTED,
                createPolicy(PROCESSING_TIMEOUT_TWENTY_MINUTES,
                        createStepExecution(startTime, minutesAgo(21), BatchStatus.COMPLETED),
                        createStepExecution(startTime, minutesAgo(22), BatchStatus.FAILED),
                        createStepExecution(startTime, minutesAgo(23), BatchStatus.ABANDONED),
                        createStepExecution(startTime, minutesAgo(22), BatchStatus.STOPPED))
                        .shouldTimeout(startTime));
    }

    @Test
    public void wholeProcessingUnknownInTimeout() {
        final long startTime = minutesAgo(24);
        Assert.assertTrue(TIMEOUT_EXPECTED,
                createPolicy(PROCESSING_TIMEOUT_TWENTY_MINUTES,
                        createStepExecution(startTime, minutesAgo(21), BatchStatus.UNKNOWN))
                        .shouldTimeout(startTime));
    }

    @Test
    public void stepExecutionJustFinishedBeforeTimeout() {
        final long startTime = minutesAgo(24);
        final StepExecution stepExecution = createStepExecution(startTime,
                minutesAgo(21), BatchStatus.COMPLETED);

        final StepExecutionAggregationTimeoutPolicy policy =
                createPolicy(PROCESSING_TIMEOUT_TWENTY_MINUTES, stepExecution);

        // simulate that the step execution has just been received, before calling shouldTimeout
        policy.onItemRegistration(new StepExecutionResult(new StepExecutionRequest(STEP_NAME, stepExecution)));

        Assert.assertFalse(NO_TIMEOUT_EXPECTED, policy.shouldTimeout(startTime));
    }

    protected StepExecutionAggregationTimeoutPolicy createPolicy(long processingTimeout,
                                                                 StepExecution... stepExecutions) {
        final List<StepExecutionRequest> executionRequest = new ArrayList<StepExecutionRequest>();

        for (StepExecution stepExecution : stepExecutions) {
            executionRequest.add(new StepExecutionRequest(STEP_NAME, stepExecution));
        }

        return new StepExecutionAggregationTimeoutPolicy(executionRequest,
                new TestableJobExplorer(Arrays.asList(stepExecutions)), processingTimeout);
    }

    protected StepExecution createStepExecution(long startTime, long lastUpdateDate, BatchStatus status) {
        final StepExecution stepExecution = new StepExecution(STEP_NAME,
                new JobExecution(JOB_EXECUTION_ID), currentStepExecutionId++);

        stepExecution.setStartTime(new Date(startTime));
        stepExecution.setLastUpdated(new Date(lastUpdateDate));
        stepExecution.setStatus(status);

        return stepExecution;
    }

    protected long minutesAgo(int minutesAgo) {
        final Calendar calendar = Calendar.getInstance();
        final int msAgo = minutesAgo * 60 * 1000;

        calendar.setTimeInMillis(calendar.getTimeInMillis() - msAgo);

        return calendar.getTime().getTime();
    }


    protected class TestableJobExplorer extends SimpleJobExplorer {
        private final Map<Long, StepExecution> stepExecutionMap = new HashMap<Long, StepExecution>();

        public TestableJobExplorer(List<StepExecution> stepExecutions) {
            super(new MapJobInstanceDao(), new MapJobExecutionDao(),
                    new MapStepExecutionDao(), new MapExecutionContextDao());
            for (StepExecution stepExecution : stepExecutions) {
                stepExecutionMap.put(stepExecution.getId(), stepExecution);
            }
        }

        @Override
        public StepExecution getStepExecution(Long jobExecutionId, Long executionId) {
            if (stepExecutionMap.containsKey(executionId)) {
                return stepExecutionMap.get(executionId);
            } else {
                throw new IllegalArgumentException("The step execution [" + executionId + "] " +
                        "cannot be found in " + stepExecutionMap.keySet());
            }
        }
    }

}
