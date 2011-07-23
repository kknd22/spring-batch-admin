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

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.NoSuchJobExecutionException;
import org.springframework.batch.core.step.NoSuchStepException;
import org.springframework.batch.integration.partition.StepExecutionRequest;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link StepExecutionRequestMapper}.
 *
 * @author Sebastien Gerard
 */
public class DefaultStepExecutionRequestMapper implements StepExecutionRequestMapper {

    private JobExplorer jobExplorer;

    public DefaultStepExecutionRequestMapper() {
    }

    public DefaultStepExecutionRequestMapper(JobExplorer jobExplorer) {
        this.jobExplorer = jobExplorer;
    }

    public StepExecution map(final StepExecutionRequest request) throws NoSuchJobExecutionException, NoSuchStepException {
        Assert.notNull(request, "The step execution request cannot be null");

        // be careful, that is the only way to get a step execution and its job execution with a context!
        final JobExecution jobExecution = jobExplorer.getJobExecution(request.getJobExecutionId());

        if (jobExecution == null) {
            throw new NoSuchJobExecutionException("The job execution with id [" + request.getJobExecutionId()
                    + "] cannot be found");
        }

        for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
            if (stepExecution.getId().equals(request.getStepExecutionId())) {
                return stepExecution;
            }
        }

        throw new NoSuchStepException("Cannot find the step execution with id [" + request.getStepExecutionId() +
                "] (job execution id is [" + request.getJobExecutionId() + "] with known execution(s) [" +
                jobExecution.getStepExecutions() + "]");
    }

    /**
     * Sets the explorer to use to get the {@link StepExecution}.
     *
     * @param jobExplorer the explorer to use
     */
    @Required
    public void setJobExplorer(final JobExplorer jobExplorer) {
        Assert.notNull(jobExplorer, "The job explorer cannot be null");
        this.jobExplorer = jobExplorer;
    }

}
