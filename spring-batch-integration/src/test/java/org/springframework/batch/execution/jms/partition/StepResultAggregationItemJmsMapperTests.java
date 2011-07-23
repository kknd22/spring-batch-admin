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

import org.apache.activemq.command.ActiveMQObjectMessage;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.batch.integration.partition.StepExecutionRequest;
import org.springframework.batch.integration.partition.StepExecutionResult;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageNotWriteableException;
import java.io.Serializable;

/**
 * @author Sebastien Gerard
 */
public class StepResultAggregationItemJmsMapperTests {

    protected static final String EXCEPTION_NOT_THROWN_MSG = "An exception should have been thrown";

    private final StepResultAggregationItemJmsMapper mapper = new StepResultAggregationItemJmsMapper();

    @Test
    public void nullMessage() throws JMSException {
        try {
            mapper.map(null);
            Assert.fail(EXCEPTION_NOT_THROWN_MSG);
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void nullPayload() throws JMSException {
        try {
            mapper.map(createTestMessage(null));
            Assert.fail(EXCEPTION_NOT_THROWN_MSG);
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void notAStepResult() throws JMSException {
        try {
            mapper.map(createTestMessage("tralala"));
            Assert.fail(EXCEPTION_NOT_THROWN_MSG);
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void stepResult() throws JMSException {
        final StepExecutionResult payload = new StepExecutionResult(new StepExecutionRequest("myStep", 1234L, 9876L));
        final StepExecutionResult unMapped = mapper.map(createTestMessage(payload));

        Assert.assertEquals("Unexpected payload", payload.getRequest().getStepName(), unMapped.getRequest().getStepName());
    }

    protected Message createTestMessage(Serializable payload) {
        try {
            final ActiveMQObjectMessage message = new ActiveMQObjectMessage();
            message.setObject(payload);
            return message;
        } catch (MessageNotWriteableException e) {
            throw new IllegalStateException(e);
        } catch (JMSException e) {
            throw new IllegalStateException(e);
        }
    }

}
