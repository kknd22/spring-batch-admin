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


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.execution.aggregation.core.AggregationItemListener;
import org.springframework.batch.execution.aggregation.core.AggregationTimeoutPolicy;
import org.springframework.batch.integration.partition.StepExecutionRequest;
import org.springframework.batch.integration.partition.StepExecutionResult;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Implementation checking that at least one {@link org.springframework.batch.core.StepExecution} in the given set of
 * requests is not in timeout. In other words, at least one {@link org.springframework.batch.core.StepExecution} is
 * still alive. A {@link org.springframework.batch.core.StepExecution} is in timeout when there is no activity
 * on the execution since a certain amount of time. An activity means any update
 * on the step execution. For instance, when a chunk is completed.
 *
 * @author Sebastien Gerard
 */
public class StepExecutionAggregationTimeoutPolicy implements AggregationTimeoutPolicy,
        AggregationItemListener<StepExecutionResult> {

    private final Log logger = LogFactory.getLog(StepExecutionAggregationTimeoutPolicy.class);

    private final Object lock = new Object();
    private final List<StepExecutionRequest> remainingRequests;
    private final JobExplorer jobExplorer;
    private final long stepExecutionTimeout;

    /**
     * Constructs the policy with all the needed information.
     *
     * @param requests the initial requests to monitor
     * @param jobExplorer the job explorer to use
     * @param stepExecutionTimeout the step execution timeout to use
     */
    public StepExecutionAggregationTimeoutPolicy(List<StepExecutionRequest> requests, JobExplorer jobExplorer,
                                                 long stepExecutionTimeout) {
        Assert.state(stepExecutionTimeout > 0, "" +
                "The timeout must be greater than 0, but was " + stepExecutionTimeout + ".");

        this.remainingRequests = new ArrayList<StepExecutionRequest>(requests);
        this.jobExplorer = jobExplorer;
        this.stepExecutionTimeout = stepExecutionTimeout;
    }

    public boolean shouldTimeout(long startTime) {
        synchronized (lock) {
            for (StepExecutionRequest stepExecutionRequest : getRemainingRequests()) {
                // TODO we can improve performance
                final StepExecution stepExecution = getJobExplorer().getStepExecution(stepExecutionRequest.getJobExecutionId(),
                        stepExecutionRequest.getStepExecutionId());
                if (!shouldTimeoutStepExecution(stepExecution)) {
                    return false;
                }
            }
        }

        return remainingRequests.size() > 0;
    }

    public void onItemRegistration(StepExecutionResult result) {
        if (logger.isDebugEnabled()) {
            logger.debug("A step execution produces a result [" + result + "]");
        }

        synchronized (lock) {
            if (!getRemainingRequests().remove(result.getRequest())) {
                logger.warn("Received step execution request [" + result.getRequest() + "] is not known!");
            }
        }
    }

    /**
     * Checks that the given step execution time does not exceed.
     *
     * @param stepExecution the given step execution
     * @return <tt>true</tt> if the step execution time exceeds, otherwise <tt>false</tt>
     */
    protected boolean shouldTimeoutStepExecution(StepExecution stepExecution) {
        final Date lastStepUpdate = stepExecution.getLastUpdated();

        if (lastStepUpdate != null) {
            final long lastUpdatedDelta = System.currentTimeMillis() - lastStepUpdate.getTime();
            if (logger.isTraceEnabled()) {
                logger.trace("Step execution [" + stepExecution.getStepName() + " (status " +
                        stepExecution.getStatus() + "] last update was " + lastStepUpdate + "ms ago.");
            }
            return lastUpdatedDelta > getStepExecutionTimeout();
        } else {
            throw new IllegalStateException("The date when the step execution was updated for the last time cannot be null [" +
                    stepExecution + "].");
        }
    }

    protected long getStepExecutionTimeout() {
        return stepExecutionTimeout;
    }

    protected List<StepExecutionRequest> getRemainingRequests() {
        return remainingRequests;
    }

    protected JobExplorer getJobExplorer() {
        return jobExplorer;
    }

}
