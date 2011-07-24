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
package org.springframework.batch.execution.jms.partition;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.execution.jms.partition.support.ReplyAwareMessageListener;
import org.springframework.batch.integration.partition.StepExecutionRequest;
import org.springframework.batch.integration.partition.StepExecutionRequestHandler;
import org.springframework.batch.integration.partition.StepExecutionResult;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.jms.core.MessageCreator;
import org.springframework.util.Assert;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import java.util.Collections;

/**
 * Listener of step execution requests. Requests processing is delegated to
 * {@link StepExecutionRequestHandler}. Once processed, the listener is
 * responsible of sending back a result to the destination mentioned in
 * the request (see {@link javax.jms.Message#getJMSReplyTo()}).
 *
 * @author Sebastien Gerard
 */
public class StepExecutionRequestMessageListener extends ReplyAwareMessageListener {

    private final Log logger = LogFactory.getLog(StepExecutionRequestMessageListener.class);

    private StepExecutionRequestHandler stepExecutionRequestHandler;

    protected MessageCreator processMessage(Message message) throws JMSException {
        Assert.notNull(message, "The message cannot be null");
        if (message instanceof ObjectMessage) {
            try {
                final Object payload = ((ObjectMessage) message).getObject();

                if (payload instanceof StepExecutionRequest) {
                    final StepExecutionRequest request = (StepExecutionRequest) payload;

                    final StepExecutionResult result = execute(request);

                    if (logger.isDebugEnabled()) {
                        logger.debug("Step execution is successful, got [" + result + "]");
                    }

                    return createResponseMessageCreator(result);
                } else {
                    logger.error("The payload of the message is not a step execution request, but was " +
                            payload);
                }
            } catch (JMSException e) {
                logger.error("Error while accessing the JMS message", e);
            }
        } else {
            logger.error("The message must be an object message, but was " + message);
        }

        return null;
    }

    /**
     * Executes the specified {@link StepExecutionRequest}.
     *
     * @param stepExecutionRequest the request
     * @return the result of the execution
     */
    protected StepExecutionResult execute(StepExecutionRequest stepExecutionRequest) {
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Executing [" + stepExecutionRequest + "]");
            }
            return stepExecutionRequestHandler.handle(stepExecutionRequest);
        } catch (Exception e) {
            logger.error("Failed to execute [" + stepExecutionRequest + "]", e);

            return new StepExecutionResult(stepExecutionRequest, Collections.<Throwable>singletonList(e));
        }
    }

    @Override
    protected void sendReply(MessageCreator response, Destination replyDestination) {
        try {
            super.sendReply(response, replyDestination);
        } catch (Exception e) {
            logger.error("Error while sending the step execution result", e);
        }
    }

    /**
     * Creates a {@link org.springframework.jms.core.MessageCreator} wrapping the {@link StepExecutionResult}.
     *
     * @param result the result of the step execution
     * @return the creator of the response
     */
    private MessageCreator createResponseMessageCreator(final StepExecutionResult result) {
        return new MessageCreator() {
            public Message createMessage(Session session) throws JMSException {
                return session.createObjectMessage(result);
            }
        };
    }

    /**
     * Sets the handler processing requests.
     *
     * @param stepExecutionRequestHandler the handler to use
     */
    @Required
    public void setStepExecutionRequestHandler(StepExecutionRequestHandler stepExecutionRequestHandler) {
        this.stepExecutionRequestHandler = stepExecutionRequestHandler;
    }

}
