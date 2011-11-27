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
package org.springframework.batch.integration.partition.amqp;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.UncategorizedAmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.ChannelCallback;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.batch.execution.aggregation.amqp.AmqpAggregationContext;
import org.springframework.batch.execution.aggregation.amqp.AmqpAggregationContextBuilder;
import org.springframework.batch.execution.aggregation.amqp.AmqpAggregationService;
import org.springframework.batch.execution.aggregation.core.AggregationItemMapper;
import org.springframework.batch.execution.support.amqp.ExtendedRabbitTemplate;
import org.springframework.batch.integration.partition.AbstractStepExecutionRequestAggregator;
import org.springframework.batch.integration.partition.StepExecutionRequest;
import org.springframework.batch.integration.partition.StepExecutionResult;
import org.springframework.beans.factory.annotation.Required;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * Aggregator based on AMQP. Messages are send to the queue
 * {@link #stepExecutionRequestQueueName}. Responses are expected on a temporary
 * queue (dedicated to the current channel). The aggregator is responsible of
 * waiting for those responses.
 *
 * @author Stephane Nicoll
 */
public class AmqpStepExecutionRequestAggregator extends AbstractStepExecutionRequestAggregator<Message> {

    private ExtendedRabbitTemplate amqpTemplate;
    private final AggregationItemMapper<Message, StepExecutionResult> aggregationItemMapper =
            new StepResultAggregationItemAmqpMapper();
    private final AmqpAggregationService amqpAggregationService = new AmqpAggregationService();
    private final MessageConverter messageConverter = new SimpleMessageConverter();

    public AmqpStepExecutionRequestAggregator() {
        super("queue.stepExecutionRequestQueue");
    }

    protected List<StepExecutionResult> doAggregate(final List<StepExecutionRequest> requests) {
        try {
            return amqpTemplate.execute(new ChannelCallback<List<StepExecutionResult>>() {
                public List<StepExecutionResult> doInRabbit(Channel channel) throws Exception {
                    return sendAndAggregate(requests, channel);
                }
            });
        } catch (UncategorizedAmqpException e) {
            if (e.getCause() != null && e.getCause() instanceof TimeoutException) {
                throw new TimeoutWrappingException(e.getCause());
            } else {
                throw e;
            }
        }
    }

    /**
     * Does the actual aggregation.
     *
     * @param requests the requests to be sent
     * @param channel the channel to use
     * @return the aggregated results.
     * @throws java.util.concurrent.TimeoutException if the aggregation time exceeds
     * @throws IOException if something failed while aggregating the results
     */
    protected List<StepExecutionResult> sendAndAggregate(List<StepExecutionRequest> requests, Channel channel)
            throws TimeoutException, IOException {
        final String replyDestination = createTemporaryDestination(channel);

        sendRequests(channel, requests, replyDestination);

        final AmqpAggregationContext<StepExecutionResult> context = AmqpAggregationContextBuilder
                .forDestination(StepExecutionResult.class, channel, replyDestination)
                .withAggregationItemMapper(aggregationItemMapper)
                .withCompletionPolicy(createCompletionPolicy(requests))
                .withTimeoutPolicy(createTimeoutPolicy(requests))
                .withReceiveTimeout(getReceiveTimeout())
                .build();

        return amqpAggregationService.aggregate(context);
    }

    /**
     * Sends the given requests to the request queue {@link #stepExecutionRequestQueueName}.
     *
     * @param channel the current channel
     * @param requests the requests to send
     * @param replyDestination the destination where results must be sent
     * @throws org.springframework.amqp.AmqpException if the requests could not be sent
     */
    protected void sendRequests(Channel channel, List<StepExecutionRequest> requests,
                                final String replyDestination) {
        final List<Message> messages = new ArrayList<Message>();
        for (final StepExecutionRequest request : requests) {
            messages.add(createMessage(request, replyDestination));
        }
        amqpTemplate.send(messages, channel, null, getStepExecutionRequestQueueName());
    }


    /**
     * Creates a temporary queue for the specified {@link Channel}.
     *
     * @param channel the channel to use
     * @return the name of a temporary queue
     * @throws IOException if the queue could not be created
     */
    protected String createTemporaryDestination(Channel channel) throws IOException {
        AMQP.Queue.DeclareOk queueDeclaration = channel.queueDeclare();
        return queueDeclaration.getQueue();
    }

    /**
     * Returns the {@link MessageConverter} to use.
     *
     * @return the message converter
     */
    protected MessageConverter getMessageConverter() {
        return messageConverter;
    }

    private Message createMessage(StepExecutionRequest request, String replyDestination) {
        final Message result = getMessageConverter().toMessage(request, new MessageProperties());
        result.getMessageProperties().setReplyTo(replyDestination);
        return enrichMessage(result);
    }

    /**
     * Sets the utility class providing rabbit friendly operations.
     *
     * @param amqpTemplate the rabbit utility class to use
     */
    @Required
    public void setAmqpTemplate(ExtendedRabbitTemplate amqpTemplate) {
        this.amqpTemplate = amqpTemplate;
    }


}