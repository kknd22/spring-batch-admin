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
package org.springframework.batch.execution.support.jms;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jms.InvalidDestinationException;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.jms.support.JmsUtils;
import org.springframework.util.Assert;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;

/**
 * A base {@link javax.jms.MessageListener} that can send a response if it is available
 * and if the <tt>JMSReplyTo</tt> header of the message is set.
 *
 * @author Stephane Nicoll
 * @see javax.jms.Message#getJMSReplyTo()
 */
public abstract class ReplyAwareMessageListener implements MessageListener {

    private final Log logger = LogFactory.getLog(ReplyAwareMessageListener.class);

    private JmsTemplate jmsTemplate;

    /**
     * Processes the specified <tt>message</tt> and returns a response for that particular
     * message as a {@link MessageCreator}. Returns <tt>null</tt> if no response must be
     * sent for that particular message.
     *
     * @param message the message to process
     * @return the response for that message or <tt>null</tt> if no response is needed
     * @throws JMSException if any exception occurs while manipulating the message
     */
    protected abstract MessageCreator processMessage(Message message) throws JMSException;

    public final void onMessage(Message message) {
        try {
            final MessageCreator response = processMessage(message);
            if (response == null) {
                logger.debug("Message processing is successful (no response)");
                return;
            }
            // Now the processing is done, let's send a reply if the replyTo is set
            if (message.getJMSReplyTo() == null) {
                logger.warn("Could not send reply as replyTo is not set in [" + message + "]");
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Sending response to [" + message.getJMSReplyTo() + "]");
                }
                sendReply(response, message.getJMSReplyTo());
            }
        } catch (JMSException e) {
           throw JmsUtils.convertJmsAccessException(e);
        }
    }

    /**
     * Sends a response to the reply destination.
     * <p/>
     * Handles the case where the reply destination is a temporary destination that
     * does not exist anymore. In that case, an error is logged.
     *
     * @param response the response to send
     * @param replyDestination the reply destination
     */
    protected void sendReply(MessageCreator response, Destination replyDestination) {
        Assert.notNull(response, "The response could not be null.");
        Assert.notNull(replyDestination, "The reply destination could not be null.");
        try {
            jmsTemplate.send(replyDestination, response);
            logger.info("Response has been sent to [" + replyDestination + "]");
        } catch (InvalidDestinationException e) {
            logger.warn("Could not send reply to [" + replyDestination + "] as the destination does not exist (anymore).");
        }

    }

    // Setter injection

    /**
     * Sets the {@link JmsTemplate} to use.
     *
     * @param jmsTemplate the jms template to use
     */
    public void setJmsTemplate(JmsTemplate jmsTemplate) {
        this.jmsTemplate = jmsTemplate;
    }

}
