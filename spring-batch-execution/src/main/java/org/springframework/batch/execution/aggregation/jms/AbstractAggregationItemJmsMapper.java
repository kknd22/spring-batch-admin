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
package org.springframework.batch.execution.aggregation.jms;


import org.springframework.batch.execution.aggregation.core.AggregationItemMapper;
import org.springframework.batch.execution.aggregation.core.MessageConversionException;

import javax.jms.JMSException;
import javax.jms.Message;

/**
 * A convenient base class for JMS-related {@link AggregationItemMapper}.
 *
 * @author Stephane Nicoll
 */
public abstract class AbstractAggregationItemJmsMapper<T> implements AggregationItemMapper<Message, T> {

    public T map(Message message) throws MessageConversionException {
        try {
            return doMap(message);
        } catch (JMSException e) {
            throw new MessageConversionException("Failed to convert incoming jms message", e);
        }
    }

    protected abstract T doMap(Message message) throws JMSException;
}
