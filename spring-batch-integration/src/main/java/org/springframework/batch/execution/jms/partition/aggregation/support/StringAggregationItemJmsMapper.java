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
package org.springframework.batch.execution.jms.partition.aggregation.support;

import org.springframework.batch.execution.jms.partition.aggregation.AggregationItemJmsMapper;
import org.springframework.util.Assert;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

/**
 * Maps the body of a {@link javax.jms.TextMessage} to a simple <tt>String</tt>.
 *
 * @author Stephane Nicoll
 */
public class StringAggregationItemJmsMapper implements AggregationItemJmsMapper<String> {

    public String map(Message message) throws JMSException {
        Assert.notNull(message, "message could not be null.");
        if (message instanceof TextMessage) {
            return ((TextMessage) message).getText();
        } else {
            throw new IllegalArgumentException("Expected text message but got [" + message + "]");
        }
    }
}
