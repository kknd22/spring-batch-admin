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
package org.springframework.batch.execution.support.amqp;

import com.rabbitmq.client.Channel;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.RabbitUtils;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.MessagePropertiesConverter;

import java.io.IOException;
import java.util.List;

/**
 * An extension o the {@link RabbitTemplate} used to send multiples
 * messages object using an existing channel.
 *
 * @author Stephane Nicoll
 */
public class ExtendedRabbitTemplate extends RabbitTemplate {

    // Note: these are copy/pasted from the parent because of private access with no getter.
    private static final String DEFAULT_EXCHANGE = ""; // alias for amq.direct default exchange
    private static final String DEFAULT_ROUTING_KEY = "";
    private static final String DEFAULT_ENCODING = "UTF-8";

    private volatile String encoding = DEFAULT_ENCODING;
    private volatile String exchange = DEFAULT_EXCHANGE;
    private volatile String routingKey = DEFAULT_ROUTING_KEY;

    private volatile MessagePropertiesConverter messagePropertiesConverter =
            new DefaultMessagePropertiesConverter();


    /**
     * Sends the specified <tt>messages</tt> instances to a specific exchange with a
     * specific routing key.
     * <p/>
     * The caller is responsible to manage the life cycle of the {@link Channel}.
     *
     * @param messages   the messages to send
     * @param channel    the channel to use
     * @param exchange   the name of the exchange
     * @param routingKey the routing key
     * @throws IllegalArgumentException if any argument is <tt>null</tt>
     * @throws org.springframework.amqp.AmqpException if the messages could not be sent
     */
    public void send(List<Message> messages, final Channel channel, String exchange, String routingKey)
            throws AmqpException {
        if (exchange == null) {
            // try to send to configured exchange
            exchange = this.exchange;
        }

        if (routingKey == null) {
            // try to send to configured routing key
            routingKey = this.routingKey;
        }

        try {
            for (Message message : messages) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Publishing message on exchange [" + exchange + "], routingKey = [" + routingKey + "]");
                }
                channel.basicPublish(exchange, routingKey, false, false,
                        this.messagePropertiesConverter.fromMessageProperties(message.getMessageProperties(), encoding),
                        message.getBody());
            }
            // Check if commit needed
            if (isChannelLocallyTransacted(channel)) {
                // Transacted channel created by this template -> commit.
                RabbitUtils.commitIfNecessary(channel);
            }
        } catch (IOException e) {
            throw RabbitUtils.convertRabbitAccessException(e);
        }
    }


    @Override
    public void setEncoding(String encoding) {
        super.setEncoding(encoding);
        this.encoding = encoding;
    }

    @Override
    public void setExchange(String exchange) {
        super.setExchange(exchange);
        this.exchange = exchange;
    }

    @Override
    public void setRoutingKey(String routingKey) {
        super.setRoutingKey(routingKey);
        this.routingKey = routingKey;
    }
}
