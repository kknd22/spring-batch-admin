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

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.launch.NoSuchJobExecutionException;
import org.springframework.batch.core.partition.support.DefaultStepExecutionAggregator;
import org.springframework.batch.core.step.NoSuchStepException;
import org.springframework.batch.integration.partition.StepExecutionResult;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.util.Assert;

import java.util.Collection;
import java.util.Collections;

/**
 * Default implementation of {@link StepExecutionResultMapper}.
 *
 * @author Sebastien Gerard
 */
public class DefaultStepExecutionResultMapper implements StepExecutionResultMapper {

    private StepExecutionRequestMapper requestMapper;
    private final DefaultStepExecutionAggregator defaultStepExecutionAggregator;

    public DefaultStepExecutionResultMapper() {
        this.defaultStepExecutionAggregator  = new DefaultStepExecutionAggregator();
    }

    public DefaultStepExecutionResultMapper(StepExecutionRequestMapper requestMapper) {
        this();
        this.requestMapper = requestMapper;
    }

    public StepExecution map(StepExecutionResult result, StepExecution masterStepExecution)
            throws NoSuchJobExecutionException, NoSuchStepException {
        Assert.notNull(result, "The step execution result cannot be null");
        Assert.notNull(masterStepExecution, "The master step execution cannot be null");

        final StepExecution stepExecution = requestMapper.map(result.getRequest());

        return updateStepExecution(masterStepExecution, result, stepExecution);
    }

    /**
     * TODO should be removed
     * This method is a workaround for <a href="">BATCH-XXX</a>
     */
    protected StepExecution updateStepExecution(StepExecution masterStepExecution, StepExecutionResult result,
                                                StepExecution updatedStepExecution) {
        final Collection<StepExecution> allStepExecutions = masterStepExecution.getJobExecution().getStepExecutions();

        for (StepExecution stepExecution : allStepExecutions) {
            if (stepExecution.equals(updatedStepExecution)) {
                defaultStepExecutionAggregator.aggregate(stepExecution, Collections.singletonList(updatedStepExecution));

                stepExecution.setVersion(updatedStepExecution.getVersion());
                stepExecution.setExecutionContext(updatedStepExecution.getExecutionContext());
                stepExecution.getFailureExceptions().clear();
                stepExecution.getFailureExceptions().addAll(result.getFailureExceptions());
                stepExecution.setLastUpdated(updatedStepExecution.getLastUpdated());
                stepExecution.setStatus(updatedStepExecution.getStatus());
                stepExecution.setEndTime(updatedStepExecution.getEndTime());

                if (updatedStepExecution.isTerminateOnly()) {
                    stepExecution.setTerminateOnly();
                }

                return stepExecution;
            }
        }

        throw new IllegalStateException("Cannot find [" + updatedStepExecution + "] in " + allStepExecutions);
    }

    /**
     * Sets the mapper mapping {@link org.springframework.batch.integration.partition.StepExecutionRequest}
     * to {@link StepExecution}.
     *
     * @param requestMapper the mapper to use
     */
    @Required
    public void setRequestMapper(StepExecutionRequestMapper requestMapper) {
        this.requestMapper = requestMapper;
    }

}
