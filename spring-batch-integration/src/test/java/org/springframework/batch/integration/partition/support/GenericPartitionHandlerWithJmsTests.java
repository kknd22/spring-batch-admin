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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.support.MapJobRegistry;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.step.NoSuchStepException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Calendar;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephane Nicoll
 */
@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext
public class GenericPartitionHandlerWithJmsTests {

    /**
     * Name of a job that is properly configured.
     */
    public static final String JOB_ONE_NAME = "job1";

    /**
     * Name of a job that refers to a step that is not defined. Should fail
     * in the remote worker.
     */
    public static final String JOB_TWO_NAME = "job2";


    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private MapJobRegistry jobRegistry;

    @Test
    public void launch() throws JobExecutionException {
        final JobExecution jobExecution = doLaunch(getJob(JOB_ONE_NAME));
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
    }

    @Test
    public void launchWithReferenceToUnknownStep() throws JobExecutionException {
        final JobExecution jobExecution = doLaunch(getJob(JOB_TWO_NAME));
        assertEquals(BatchStatus.FAILED, jobExecution.getStatus());
        // Make sure that the exceptions are properly communicated to the master execution
        assertEquals("The 2 remote partitions should have failed as well as the master so " +
                "3 exceptions are expected here", 3, jobExecution.getAllFailureExceptions().size());
        // TODO: well this is a bit ugly and probably making too much assumptions
        for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
            assertEquals("One failure exception should be available", 1, stepExecution.getFailureExceptions().size());
            if (stepExecution.getStepName().contains("partition")) {
                assertEquals("Wrong failure exception for partition", NoSuchStepException.class,
                        stepExecution.getFailureExceptions().get(0).getClass());
            } else {
               assertEquals("Wrong failure exception for master", JobExecutionException.class,
                        stepExecution.getFailureExceptions().get(0).getClass());
            }
        }
    }


    protected Job getJob(String jobName) {
        try {
            return jobRegistry.getJob(jobName);
        } catch (NoSuchJobException e) {
            throw new IllegalStateException("Job [" + jobName + "] should have been found", e);
        }
    }

    protected JobExecution doLaunch(Job job) throws JobExecutionException {
        Calendar c = Calendar.getInstance();
        JobParametersBuilder builder = new JobParametersBuilder();
        builder.addDate("TIMESTAMP", c.getTime());
        return jobLauncher.run(job, builder.toJobParameters());
    }
}
