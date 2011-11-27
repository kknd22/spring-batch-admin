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


import org.springframework.batch.execution.aggregation.core.AggregationItemMapper;
import org.springframework.batch.execution.aggregation.jms.JmsAggregationContext;
import org.springframework.batch.execution.aggregation.jms.JmsAggregationContextBuilder;
import org.springframework.batch.execution.aggregation.jms.JmsAggregationService;
import org.springframework.batch.execution.support.jms.ExtendedJmsTemplate;
import org.springframework.batch.integration.partition.AbstractStepExecutionRequestAggregator;
import org.springframework.batch.integration.partition.StepExecutionRequest;
import org.springframework.batch.integration.partition.StepExecutionResult;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.jms.core.MessageCreator;
import org.springframework.jms.core.SessionCallback;
import org.springframework.jms.support.JmsUtils;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * Aggregator based on JMS. Messages are send to the queue
 * {@link #stepExecutionRequestQueueName}. Responses are expected on a temporary
 * queue (dedicated to that particular execution). The aggregator is responsible of
 * waiting for those responses.
 * <p/>
 * The aggregator is smart enough to detect a timeout when there is no activity on a
 * {@link org.springframework.batch.core.StepExecution}.
 *
 * @author Sebastien Gerard
 */
public class JmsStepExecutionRequestAggregator extends AbstractStepExecutionRequestAggregator<ObjectMessage> {

    private ExtendedJmsTemplate jmsTemplate;
    private final AggregationItemMapper<Message, StepExecutionResult> aggregationItemMapper =
            new StepResultAggregationItemJmsMapper();
    private final JmsAggregationService jmsAggregationService = new JmsAggregationService();

    public JmsStepExecutionRequestAggregator() {
        super("queue/stepExecutionRequestQueue");
    }

    protected List<StepExecutionResult> doAggregate(final List<StepExecutionRequest> requests) {
        return jmsTemplate.execute(new SessionCallback<List<StepExecutionResult>>() {
                    public List<StepExecutionResult> doInJms(Session session) {
                        try {
                            return sendAndAggregate(requests, session);
                        } catch (TimeoutException e) {
                            throw new TimeoutWrappingException(e);
                        }
                    }
                }, true);
    }

    /**
     * Does the actual aggregation.
     *
     * @param requests the requests to be sent
     * @param session the current JMS session
     * @return the aggregated results.
     * @throws java.util.concurrent.TimeoutException if the aggregation time exceeds
     */
    protected List<StepExecutionResult> sendAndAggregate(List<StepExecutionRequest> requests, Session session)
            throws TimeoutException {
        final Destination replyDestination = createTemporaryDestination(session);

        sendRequests(session, requests, replyDestination);

        final JmsAggregationContext<StepExecutionResult> context = JmsAggregationContextBuilder
                .forDestination(StepExecutionResult.class, session, replyDestination)
                .withAggregationItemMapper(aggregationItemMapper)
                .withCompletionPolicy(createCompletionPolicy(requests))
                .withTimeoutPolicy(createTimeoutPolicy(requests))
                .withReceiveTimeout(getReceiveTimeout())
                .build();

        return jmsAggregationService.aggregate(context);
    }

    /**
     * Sends the given requests to the request queue {@link #stepExecutionRequestQueueName}.
     *
     * @param session the current JMS session
     * @param requests the requests to send
     * @param replyDestination the destination where results must be sent
     * @throws org.springframework.jms.JmsException if the requests could not be sent
     */
    protected void sendRequests(Session session, List<StepExecutionRequest> requests,
                                final Destination replyDestination) {
        final List<MessageCreator> messageCreators = new ArrayList<MessageCreator>();

        for (final StepExecutionRequest request : requests) {
            messageCreators.add(new MessageCreator() {
                public Message createMessage(Session session) throws JMSException {
                    final ObjectMessage message = session.createObjectMessage(request);

                    message.setJMSReplyTo(replyDestination);
                    return enrichMessage(message);
                }
            });
        }

        try {
            final Destination requestQueue = jmsTemplate.getDestinationResolver().
                    resolveDestinationName(session, getStepExecutionRequestQueueName(), false);
            jmsTemplate.send(messageCreators, session, requestQueue);
        } catch (JMSException e) {
            throw JmsUtils.convertJmsAccessException(e);
        }
    }


    /**
     * Creates a temporary destination.
     *
     * @param session the current JMS session
     * @return a temporary destination in the scope of the current session
     */
    protected Destination createTemporaryDestination(Session session) {
        try {
            return session.createTemporaryQueue();
        } catch (JMSException e) {
            throw new IllegalStateException("Error while creating a temporary queue", e);
        }
    }

    /**
     * Sets the utility class providing JMS friendly operations.
     *
     * @param jmsTemplate the JMS utility class to use
     */
    @Required
    public void setJmsTemplate(ExtendedJmsTemplate jmsTemplate) {
        this.jmsTemplate = jmsTemplate;
    }

}