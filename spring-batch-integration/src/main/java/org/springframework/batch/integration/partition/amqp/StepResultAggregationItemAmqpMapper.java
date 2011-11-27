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
import org.springframework.batch.execution.aggregation.amqp.AbstractAggregationItemAmqpMapper;
import org.springframework.batch.integration.partition.StepExecutionResult;

import java.io.IOException;

/**
 * Mapper responsible of extracting a {@link StepExecutionResult} from an amqp {@link Message}.
 *
 * @author Stephane Nicoll
 */
class StepResultAggregationItemAmqpMapper extends AbstractAggregationItemAmqpMapper<StepExecutionResult> {

    @Override
    protected StepExecutionResult doMap(Message message) throws IOException {
        final Object o = getMessageConverter().fromMessage(message);
        if (o instanceof StepExecutionResult) {
            return (StepExecutionResult) o;
        } else {
            throw new IllegalArgumentException("Expected step execution result but got [" + o + "]");
        }
    }

}