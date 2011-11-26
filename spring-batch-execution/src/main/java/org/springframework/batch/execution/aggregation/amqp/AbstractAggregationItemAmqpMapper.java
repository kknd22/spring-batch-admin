package org.springframework.batch.execution.aggregation.amqp;

import org.springframework.amqp.core.Message;
import org.springframework.batch.execution.aggregation.core.AggregationItemMapper;
import org.springframework.batch.execution.aggregation.core.MessageConversionException;
import org.springframework.util.Assert;

import java.io.IOException;


/**
 * A convenient base class for AMQP-related {@link AggregationItemMapper}.
 *
 * @author Stephane Nicoll
 */
public abstract class AbstractAggregationItemAmqpMapper<T> implements AggregationItemMapper<Message, T> {

    public T map(Message message) throws MessageConversionException {
        Assert.notNull(message, "message could not be null.");
        try {
            return doMap(message);
        } catch (IOException e) {
            throw new MessageConversionException("Failed to convert incoming amqp message", e);
        }
    }

     /**
     * Maps the specified message.
     *
     * @param message the amqp message (never null)
     * @return the mapped item
     * @throws IOException if the message could not be read properly
     */
    protected abstract T doMap(Message message) throws IOException;

}

