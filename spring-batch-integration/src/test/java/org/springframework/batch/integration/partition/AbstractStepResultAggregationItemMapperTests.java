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

import org.junit.Assert;
import org.junit.Test;
import org.springframework.batch.execution.aggregation.core.AggregationItemMapper;

import javax.jms.JMSException;
import java.io.Serializable;

/**
 * Shared tests for {@link StepExecutionResult} {@link AggregationItemMapper}.
 *
 * @author Stephane Nicoll
 */
public abstract class AbstractStepResultAggregationItemMapperTests<M> {

    @Test(expected = IllegalArgumentException.class)
    public void nullMessage() throws JMSException {
        getMapper().map(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void nullPayload() throws JMSException {
        getMapper().map(createTestMessage(null));
    }

    @Test(expected = IllegalArgumentException.class)
    public void notAStepResult() throws JMSException {
        getMapper().map(createTestMessage("Some dummy content"));
    }

    @Test
    public void stepResult() throws JMSException {
        final StepExecutionResult payload = new StepExecutionResult(new StepExecutionRequest("myStep", 1234L, 9876L));
        final StepExecutionResult actual = getMapper().map(createTestMessage(payload));

        Assert.assertEquals("Unexpected payload", payload.getRequest().getStepName(),
                actual.getRequest().getStepName());
    }

    protected abstract AggregationItemMapper<M, StepExecutionResult> getMapper();

    protected abstract M createTestMessage(Serializable payload);
}
