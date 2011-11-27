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
package org.springframework.batch.integration.partition.jms;

import org.apache.activemq.command.ActiveMQObjectMessage;
import org.springframework.batch.execution.aggregation.core.AggregationItemMapper;
import org.springframework.batch.integration.partition.AbstractStepResultAggregationItemMapperTests;
import org.springframework.batch.integration.partition.StepExecutionResult;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageNotWriteableException;
import java.io.Serializable;

/**
 * @author Sebastien Gerard
 */
public class StepResultAggregationItemJmsMapperTests extends AbstractStepResultAggregationItemMapperTests<Message> {

    private final StepResultAggregationItemJmsMapper mapper = new StepResultAggregationItemJmsMapper();

    @Override
    protected AggregationItemMapper<Message, StepExecutionResult> getMapper() {
        return mapper;
    }

    @Override
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
