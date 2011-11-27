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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.execution.support.jms.ReplyAwareMessageListener;
import org.springframework.batch.integration.partition.StepExecutionRequest;
import org.springframework.batch.integration.partition.StepExecutionResult;
import org.springframework.jms.core.MessageCreator;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Session;

/**
 * A test message listener used to configure two main features:
 * <ul>
 * <li>Processing time. By simulating processing time we can test timeout situations</li>
 * <li>Reply. By enabling replies on demand we can simulate when a remote worker fails without
 * being able to notify the master</li>
 * </ul>
 *
 * @author Stephane Nicoll
 */
public class StepExecutionRequestTestListener extends ReplyAwareMessageListener {

    /**
     * The key holding the time the listener should wait, in ms.
     */
    public static final String FAKE_PROCESSING_TIME_KEY = "fakeProcessingTime";

    /**
     * The key specifying if a reply should be sent.
     */
    public static final String SEND_REPLY_KEY = "reply";

    private final Log logger = LogFactory.getLog(StepExecutionRequestTestListener.class);


    @Override
    protected MessageCreator processMessage(Message message) throws JMSException {
        logger.info("Received message");

        if (message.propertyExists(FAKE_PROCESSING_TIME_KEY)) {
            final long waitMs = message.getLongProperty(FAKE_PROCESSING_TIME_KEY);
            logger.debug("Simulating processing time, waiting [" + waitMs + "ms]");
            try {
                Thread.sleep(waitMs);
            } catch (InterruptedException e) {
                throw new IllegalStateException("Message processing interrupted while sleeping");
            }
        }

        // Handle reply
        if (message.propertyExists(SEND_REPLY_KEY)) {
            logger.debug("Sending reply.");
            return doCreateReply(message);
        } else {
            return null;
        }
    }

    protected MessageCreator doCreateReply(Message originalMessage) throws JMSException {
        final StepExecutionRequest request = (StepExecutionRequest) ((ObjectMessage) originalMessage).getObject();

        return new MessageCreator() {
            public Message createMessage(Session session) throws JMSException {
                return session.createObjectMessage(new StepExecutionResult(request));
            }
        };
    }
}