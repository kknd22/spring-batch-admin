package org.springframework.batch.execution.aggregation.amqp.support;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.batch.execution.aggregation.amqp.AbstractAggregationItemAmqpMapper;

import java.io.IOException;

/**
 * Maps the body of a {@link Message} to a simple <tt>String</tt>.
 *
 * @author Stephane Nicoll
 */
public class StringAggregationItemAmqpMapper extends AbstractAggregationItemAmqpMapper<String> {

    private final MessageConverter messageConverter = new SimpleMessageConverter();

    @Override
    protected String doMap(Message message) throws IOException {
        final Object o = messageConverter.fromMessage(message);
        if (o instanceof String) {
            return (String) o;
        } else {
            throw new IllegalArgumentException("Expected String message but got [" + o + "]");
        }
    }
}
