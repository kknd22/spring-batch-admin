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


import org.springframework.batch.execution.aggregation.jms.AbstractAggregationItemJmsMapper;
import org.springframework.batch.integration.partition.StepExecutionResult;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import java.io.Serializable;

/**
 * Mapper responsible of extracting a {@link StepExecutionResult} from an {@link javax.jms.ObjectMessage}.
 *
 * @author Sebastien Gerard
 */
class StepResultAggregationItemJmsMapper extends AbstractAggregationItemJmsMapper<StepExecutionResult> {

    public StepExecutionResult doMap(Message message) throws JMSException {
        if ((message != null) && message instanceof ObjectMessage) {
            final Serializable serializable = ((ObjectMessage) message).getObject();

            if (serializable instanceof StepExecutionResult) {
                return (StepExecutionResult) serializable;
            } else {
                throw new IllegalArgumentException("Expected step execution result but got [" + serializable + "]");
            }
        } else {
            throw new IllegalArgumentException("Expected object message but got [" + message + "]");
        }
    }

}
