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
package org.springframework.batch.execution.aggregation.amqp.support;

import org.junit.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;

import static junit.framework.Assert.assertEquals;

/**
 * @author Stephane Nicoll
 */
public class StringAggregationItemAmqpMapperTests {

    private final MessageConverter messageConverter = new SimpleMessageConverter();
    private final StringAggregationItemAmqpMapper instance = new StringAggregationItemAmqpMapper();

    @Test
    public void mapString() {
        final String content = "foo";
        assertEquals(content, instance.map(createMessage(content)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void mapOtherObject() {
        instance.map(createMessage(new Object()));
    }


    protected Message createMessage(Object o) {
        return messageConverter.toMessage(o, new MessageProperties());
    }


}
