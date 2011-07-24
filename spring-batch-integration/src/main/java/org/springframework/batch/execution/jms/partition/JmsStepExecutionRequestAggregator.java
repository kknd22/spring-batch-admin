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
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.execution.jms.partition.aggregation.AggregationCompletionPolicy;
import org.springframework.batch.execution.jms.partition.aggregation.AggregationItemJmsMapper;
import org.springframework.batch.execution.jms.partition.aggregation.AggregationTimeoutPolicy;
import org.springframework.batch.execution.jms.partition.aggregation.JmsAggregationContext;
import org.springframework.batch.execution.jms.partition.aggregation.JmsAggregationContextBuilder;
import org.springframework.batch.execution.jms.partition.aggregation.JmsAggregationService;
import org.springframework.batch.execution.jms.partition.aggregation.support.CountBasedAggregationCompletionPolicy;
import org.springframework.batch.execution.jms.partition.support.ExtendedJmsTemplate;
import org.springframework.batch.integration.partition.StepExecutionRequest;
import org.springframework.batch.integration.partition.StepExecutionRequestAggregator;
import org.springframework.batch.integration.partition.StepExecutionResult;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.jms.core.MessageCreator;
import org.springframework.jms.core.SessionCallback;
import org.springframework.jms.support.JmsUtils;
import org.springframework.util.Assert;

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
public class JmsStepExecutionRequestAggregator implements StepExecutionRequestAggregator {

    private final Log logger = LogFactory.getLog(JmsStepExecutionRequestAggregator.class);

    public static final long DEFAULT_STEP_EXECUTION_TIMEOUT = 20 * 60 * 1000L; // 20 minutes

    private ExtendedJmsTemplate jmsTemplate;
    private JobExplorer jobExplorer;

    private String stepExecutionRequestQueueName = "queue/stepExecutionRequestQueue";
    private Long receiveTimeout = JmsAggregationContextBuilder.DEFAULT_RECEIVE_TIMEOUT;
    private long stepExecutionTimeout = DEFAULT_STEP_EXECUTION_TIMEOUT;

    public List<StepExecutionResult> aggregate(final List<StepExecutionRequest> requests) throws TimeoutException {
        Assert.notNull(requests, "Step execution requests cannot be null");

        if (requests.size() > 0) {
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("Executing [" + requests.size() + "] step execution request(s).");
                }
                return jmsTemplate.execute(new SessionCallback<List<StepExecutionResult>>() {
                            public List<StepExecutionResult> doInJms(Session session) {
                                try {
                                    return doAggregate(requests, session);
                                } catch (TimeoutException e) {
                                    throw new TimeoutWrappingException(e);
                                }
                            }
                        }, true);
            } catch (TimeoutWrappingException e) {
                throw (TimeoutException) e.getCause();
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Empty step execution requests received, returning an empty list of result.");
            }
            return new ArrayList<StepExecutionResult>();
        }
    }

    /**
     * Does the actual aggregation.
     *
     * @param requests the requests to be sent
     * @param session the current JMS session
     * @return the aggregated results.
     * @throws java.util.concurrent.TimeoutException if the aggregation time exceeds
     */
    protected List<StepExecutionResult> doAggregate(List<StepExecutionRequest> requests, Session session)
            throws TimeoutException {
        final Destination replyDestination = createTemporaryDestination(session);

        sendRequests(session, requests, replyDestination);

        final JmsAggregationContext<StepExecutionResult> context = JmsAggregationContextBuilder
                .forDestination(StepExecutionResult.class, session, replyDestination)
                .withAggregationItemMapper(createAggregationMapper(requests))
                .withCompletionPolicy(createCompletionPolicy(requests))
                .withTimeoutPolicy(createTimeoutPolicy(requests))
                .withReceiveTimeout(receiveTimeout)
                .build();

        return new JmsAggregationService().aggregate(context);
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
                    enrichMessage(message);

                    return message;
                }
            });
        }

        try {
            final Destination requestQueue = jmsTemplate.getDestinationResolver().
                    resolveDestinationName(session, stepExecutionRequestQueueName, false);
            jmsTemplate.send(messageCreators, session, requestQueue);
        } catch (JMSException e) {
            throw JmsUtils.convertJmsAccessException(e);
        }
    }

    /**
     * Creates the mapper to use to map results from JMS messages.
     *
     * @param requests the requests to aggregate
     * @return a new mapper instance
     */
    protected AggregationItemJmsMapper<StepExecutionResult> createAggregationMapper(List<StepExecutionRequest> requests) {
        return new StepResultAggregationItemJmsMapper();
    }

    /**
     * Creates the aggregation completion policy to use.
     *
     * @param requests the requests to aggregate
     * @return a new completion policy
     */
    protected AggregationCompletionPolicy createCompletionPolicy(List<StepExecutionRequest> requests) {
        return new CountBasedAggregationCompletionPolicy(requests.size());
    }

    /**
     * Creates the aggregation timeout policy to use.
     *
     * @param requests the requests to aggregate
     * @return a new timeout policy
     */
    protected AggregationTimeoutPolicy createTimeoutPolicy(List<StepExecutionRequest> requests) {
        return new StepExecutionAggregationTimeoutPolicy(requests, jobExplorer, stepExecutionTimeout);
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
     * Enriches the message with additional headers.
     *
     * @param message the message to enrich
     */
    protected void enrichMessage(ObjectMessage message) {
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

    /**
     * Sets the explorer providing information about
     * {@link org.springframework.batch.core.StepExecution}.
     *
     * @param jobExplorer the job explorer to use
     */
    @Required
    public void setJobExplorer(JobExplorer jobExplorer) {
        this.jobExplorer = jobExplorer;
    }

    /**
     * Sets the name of the queue where {@link StepExecutionRequest} are sent.
     *
     * @param stepExecutionRequestQueueName the request queue name
     */
    public void setStepExecutionRequestQueueName(String stepExecutionRequestQueueName) {
        this.stepExecutionRequestQueueName = stepExecutionRequestQueueName;
    }

    /**
     * Sets the maximum time in seconds during the aggregator tries to read
     * a new message result.
     *
     * @param receiveTimeout the JMS message receiving timeout in seconds
     */
    public void setReceiveTimeout(long receiveTimeout) {
        Assert.state(receiveTimeout > 0, "The timeout must be greater than 0");
        // Requires ms internally
        this.receiveTimeout = receiveTimeout * 1000;
    }

    /**
     * Sets the maximum time in seconds when a step execution that has not lead to a
     * {@link StepExecutionResult} is allowed to have no visible update.
     * After that the step execution is considered as dead.
     *
     * @param stepExecutionTimeout the step execution timeout in seconds to use
     */
    public void setStepExecutionTimeout(long stepExecutionTimeout) {
        Assert.state(receiveTimeout > 0, "The timeout must be greater than 0");
        // Requires ms internally
        this.stepExecutionTimeout = stepExecutionTimeout * 1000;
    }

    protected static class TimeoutWrappingException extends RuntimeException {
        public TimeoutWrappingException(Throwable cause) {
            super(cause);
        }
    }

}