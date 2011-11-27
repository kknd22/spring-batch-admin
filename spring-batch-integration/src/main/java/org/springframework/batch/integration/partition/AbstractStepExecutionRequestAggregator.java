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
package org.springframework.batch.integration.partition;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.execution.aggregation.core.AggregationCompletionPolicy;
import org.springframework.batch.execution.aggregation.core.AggregationTimeoutPolicy;
import org.springframework.batch.execution.aggregation.core.support.BaseAggregationContextBuilder;
import org.springframework.batch.execution.aggregation.core.support.CountBasedAggregationCompletionPolicy;
import org.springframework.batch.integration.partition.support.StepExecutionAggregationTimeoutPolicy;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * A base {@link StepExecutionRequestAggregator} implementation.
 * <p/>
 * The aggregator is smart enough to detect a timeout when there is no activity on a
 * {@link org.springframework.batch.core.StepExecution}.
 *
 * @author Stephane Nicoll
 */
public abstract class AbstractStepExecutionRequestAggregator<M> implements StepExecutionRequestAggregator {

    protected final Log logger = LogFactory.getLog(getClass());

    public static final long DEFAULT_STEP_EXECUTION_TIMEOUT = 20 * 60 * 1000L; // 20 minutes

    private JobExplorer jobExplorer;
    private Long receiveTimeout = BaseAggregationContextBuilder.DEFAULT_RECEIVE_TIMEOUT;
    private long stepExecutionTimeout = DEFAULT_STEP_EXECUTION_TIMEOUT;
    private String stepExecutionRequestQueueName;

    protected AbstractStepExecutionRequestAggregator(String stepExecutionRequestQueueName) {
        this.stepExecutionRequestQueueName = stepExecutionRequestQueueName;
    }

    public List<StepExecutionResult> aggregate(final List<StepExecutionRequest> requests) throws TimeoutException {
        Assert.notNull(requests, "Step execution requests cannot be null");

        if (requests.size() > 0) {
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("Executing [" + requests.size() + "] step execution request(s).");
                }
                return doAggregate(requests);
            } catch (TimeoutWrappingException e) {
                throw (TimeoutException) e.getCause();
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Empty step execution requests received, returning an empty list of result.");
            }
            return new ArrayList<StepExecutionResult>();
        }
    }

    /**
     * Performs the actual aggregation. Only called if at least one request
     * is present in the <tt>requests</tt> list.
     *
     * @param requests the step execution request(s) to send
     * @return the aggregated results
     * @throws TimeoutWrappingException if a timeout exception occurred and should be thrown as runtime
     */
    protected abstract List<StepExecutionResult> doAggregate(final List<StepExecutionRequest> requests);

    /**
     * Creates the aggregation completion policy to use.
     *
     * @param requests the requests to aggregate
     * @return a new completion policy
     */
    protected AggregationCompletionPolicy createCompletionPolicy(List<StepExecutionRequest> requests) {
        return new CountBasedAggregationCompletionPolicy(requests.size());
    }

    /**
     * Creates the aggregation timeout policy to use.
     *
     * @param requests the requests to aggregate
     * @return a new timeout policy
     */
    protected AggregationTimeoutPolicy createTimeoutPolicy(List<StepExecutionRequest> requests) {
        return new StepExecutionAggregationTimeoutPolicy(requests, jobExplorer, stepExecutionTimeout);
    }

    /**
     * Enriches the message.
     * <p/>
     * Could also return a completely different message if necessary.
     *
     * @param message the message to enrich
     * @return the enriched message
     */
    protected M enrichMessage(M message) {
        return message;
    }

    /**
     * Returns the configured receive timeout.
     *
     * @return the message receiving timeout in ms
     */
    protected Long getReceiveTimeout() {
        return receiveTimeout;
    }

    /**
     * @return the name of the request queue
     */
    public String getStepExecutionRequestQueueName() {
        return stepExecutionRequestQueueName;
    }

    /**
     * Sets the explorer providing information about
     * {@link org.springframework.batch.core.StepExecution}.
     *
     * @param jobExplorer the job explorer to use
     */
    @Required
    public void setJobExplorer(JobExplorer jobExplorer) {
        this.jobExplorer = jobExplorer;
    }

    /**
     * Sets the maximum time in seconds during the aggregator tries to read
     * a new message result.
     *
     * @param receiveTimeout the message receiving timeout in seconds
     */
    public void setReceiveTimeout(long receiveTimeout) {
        Assert.state(receiveTimeout > 0, "The timeout must be greater than 0");
        // Requires ms internally
        this.receiveTimeout = receiveTimeout * 1000;
    }

    /**
     * Sets the maximum time in seconds when a step execution that has not lead to a
     * {@link StepExecutionResult} is allowed to have no visible update.
     * After that the step execution is considered as dead.
     *
     * @param stepExecutionTimeout the step execution timeout in seconds to use
     */
    public void setStepExecutionTimeout(long stepExecutionTimeout) {
        Assert.state(receiveTimeout > 0, "The timeout must be greater than 0");
        // Requires ms internally
        this.stepExecutionTimeout = stepExecutionTimeout * 1000;
    }

    /**
     * Sets the name of the queue where {@link StepExecutionRequest} are sent.
     *
     * @param stepExecutionRequestQueueName the request queue name
     */
    public void setStepExecutionRequestQueueName(String stepExecutionRequestQueueName) {
        this.stepExecutionRequestQueueName = stepExecutionRequestQueueName;
    }

    protected static class TimeoutWrappingException extends RuntimeException {
        public TimeoutWrappingException(Throwable cause) {
            super(cause);
        }
    }
}
