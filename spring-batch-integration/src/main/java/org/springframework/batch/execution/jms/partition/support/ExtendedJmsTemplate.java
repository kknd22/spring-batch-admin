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
package org.springframework.batch.execution.jms.partition.support;

import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.jms.support.JmsUtils;
import org.springframework.util.Assert;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.util.List;

/**
 * An extension of the regular {@link JmsTemplate}. Allows to perform
 * more fine-grained operations based on the {@link Session}.
 * <p/>
 * It is not expected this would be used outside the framework code.
 *
 * @author Stephane Nicoll
 */
public class ExtendedJmsTemplate extends JmsTemplate {

    public ExtendedJmsTemplate() {
    }

    protected ExtendedJmsTemplate(ConnectionFactory connectionFactory) {
        super(connectionFactory);
    }

    /**
     * Sends the messages produced by the specified list of {@link MessageCreator}
     * instances on the specified {@link Destination}.
     * <p/>
     * The caller is responsible to manage the lifecycle of the {@link Session}.
     *
     * @param messageCreators the message creators to use
     * @param session the session to use
     * @param destination the destination to use
     * @throws IllegalArgumentException if any argument is <tt>null</tt>
     * @throws org.springframework.jms.JmsException if the message could not be sent
     */
    public void send(List<MessageCreator> messageCreators, Session session, Destination destination) {
        Assert.notNull(messageCreators, "MessageCreator list could not be null.");
        Assert.notNull(session, "session could not be null.");
        Assert.notNull(destination, "destination could not be null.");
        try {
            MessageProducer producer = createProducer(session, destination);
            try {
                for (MessageCreator messageCreator : messageCreators) {
                    Message message = messageCreator.createMessage(session);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Sending created message: " + message);
                    }
                    doSend(producer, message);
                }
                // Check commit - avoid commit call within a JTA transaction.
                if (session.getTransacted() && isSessionLocallyTransacted(session)) {
                    // Transacted session created by this template -> commit.
                    JmsUtils.commitIfNecessary(session);
                }
            } finally {
                JmsUtils.closeMessageProducer(producer);
            }
        } catch (JMSException e) {
            throw convertJmsAccessException(e);
        }
    }

}