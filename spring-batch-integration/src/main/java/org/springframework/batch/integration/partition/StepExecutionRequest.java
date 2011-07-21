package org.springframework.batch.integration.partition;

import org.springframework.batch.core.StepExecution;
import org.springframework.util.Assert;

import java.io.Serializable;

/**
 * Materializes a request to execute a particular
 * {@link org.springframework.batch.core.Step} with a given
 * {@link org.springframework.batch.core.StepExecution}. Used to distribute
 * partition executions on multiple workers.
 *
 * @author Dave Syer
 */
public class StepExecutionRequest implements Serializable {

    private final String stepName;

    private final Long jobExecutionId;

	private final Long stepExecutionId;

    /**
     * Creates a new instance.
     *
     * @param stepName the name of the step to execute
     * @param jobExecutionId the id of the current job execution
     * @param stepExecutionId the id of the partition execution to be started
     */
	public StepExecutionRequest(String stepName, Long jobExecutionId, Long stepExecutionId) {
        Assert.notNull(stepName, "The step name cannot be null");
        this.stepName = stepName;
		this.jobExecutionId = jobExecutionId;
		this.stepExecutionId = stepExecutionId;
	}

    /**
     * Creates a request mentioning the given {@link org.springframework.batch.core.StepExecution}.
     *
     * @param actualStepName the actual name of the step to execute
     * @param stepExecution the given {@link org.springframework.batch.core.StepExecution}.
     */
    public StepExecutionRequest(final String actualStepName, final StepExecution stepExecution) {
        this(actualStepName, stepExecution.getJobExecutionId(), stepExecution.getId());
    }

    /**
     * @return the step to which the partition belongs
     */
    public String getStepName() {
		return stepName;
	}

    /**
     * @return the id of the current job execution
     */
	public Long getJobExecutionId() {
		return jobExecutionId;
	}

    /**
     * @return the id of the partition execution to be started
     */
	public Long getStepExecutionId() {
		return stepExecutionId;
	}

	@Override
	public String toString() {
		return String.format("StepExecutionRequest: [jobExecutionId=%d, stepExecutionId=%d, stepName=%s]",
				jobExecutionId, stepExecutionId, stepName);
	}

}
