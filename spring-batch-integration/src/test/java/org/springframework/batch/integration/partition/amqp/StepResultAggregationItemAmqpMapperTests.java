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
package org.springframework.batch.integration.partition.amqp;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.batch.execution.aggregation.core.AggregationItemMapper;
import org.springframework.batch.integration.partition.AbstractStepResultAggregationItemMapperTests;
import org.springframework.batch.integration.partition.StepExecutionResult;

import java.io.Serializable;

/**
 * @author Stephane Nicoll
 */
public class StepResultAggregationItemAmqpMapperTests extends AbstractStepResultAggregationItemMapperTests<Message> {

    private final StepResultAggregationItemAmqpMapper mapper = new StepResultAggregationItemAmqpMapper();
    private final MessageConverter messageConverter = new SimpleMessageConverter();

    @Override
    protected AggregationItemMapper<Message, StepExecutionResult> getMapper() {
        return mapper;
    }

    @Override
    protected Message createTestMessage(Serializable payload) {
        return messageConverter.toMessage(payload, new MessageProperties());
    }
}
