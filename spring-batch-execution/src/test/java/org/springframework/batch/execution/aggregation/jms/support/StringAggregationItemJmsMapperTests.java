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
package org.springframework.batch.execution.aggregation.jms.support;

import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.junit.Test;

import javax.jms.JMSException;
import javax.jms.MessageNotWriteableException;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

/**
 * @author Stephane Nicoll
 */
public class StringAggregationItemJmsMapperTests {

    private final StringAggregationItemJmsMapper mapper = new StringAggregationItemJmsMapper();

    @Test
    public void mapTextMessage() throws JMSException {
        final String content = "Foo";
        final ActiveMQTextMessage message = createTestMessage(content);
        assertEquals("Wrong content mapped", content, mapper.map(message));
    }

    @Test
    public void mapObjectMessage() throws JMSException {
        try {
            mapper.map(new ActiveMQObjectMessage());
            fail("Should have failed to map an object message");
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    protected ActiveMQTextMessage createTestMessage(String content) {
        try {
            final ActiveMQTextMessage message = new ActiveMQTextMessage();
            message.setText(content);
            return message;
        } catch (MessageNotWriteableException e) {
            throw new IllegalStateException(e);
        }
    }
}
