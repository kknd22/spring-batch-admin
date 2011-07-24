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
package org.springframework.batch.execution.jms;

import org.springframework.jms.core.MessageCreator;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

/**
 * An implementation of {@link org.springframework.jms.core.MessageCreator} which creates
 * a {@link javax.jms.TextMessage} with a single {@link String}.
 *
 * @author Stephane Nicoll
 */
public class TextMessageCreator implements MessageCreator {

    private final String messageContent;

    /**
     * Creates an instance with the specified content.
     *
     * @param messageContent the content to put in the jms message
     */
    public TextMessageCreator(String messageContent) {
        this.messageContent = messageContent;
    }

    public Message createMessage(Session session) throws JMSException {
        return session.createTextMessage(messageContent);
    }
}
