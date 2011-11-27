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
package org.springframework.batch.execution.aggregation.amqp;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.batch.execution.aggregation.core.AggregationItemMapper;
import org.springframework.batch.execution.aggregation.core.MessageConversionException;
import org.springframework.util.Assert;

import java.io.IOException;


/**
 * A convenient base class for AMQP-related {@link AggregationItemMapper}.
 *
 * @author Stephane Nicoll
 */
public abstract class AbstractAggregationItemAmqpMapper<T> implements AggregationItemMapper<Message, T> {

    private final MessageConverter messageConverter = new SimpleMessageConverter();

    public T map(Message message) throws MessageConversionException {
        Assert.notNull(message, "message could not be null.");
        try {
            return doMap(message);
        } catch (IOException e) {
            throw new MessageConversionException("Failed to convert incoming amqp message", e);
        }
    }

    /**
     * Maps the specified message.
     *
     * @param message the amqp message (never null)
     * @return the mapped item
     * @throws IOException if the message could not be read properly
     */
    protected abstract T doMap(Message message) throws IOException;

    /**
     * @return the simple message converter
     */
    protected MessageConverter getMessageConverter() {
        return messageConverter;
    }
}

