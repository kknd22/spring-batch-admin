package org.springframework.batch.integration.partition.amqp;

import com.rabbitmq.client.Channel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.batch.integration.partition.StepExecutionRequest;
import org.springframework.batch.integration.partition.StepExecutionResult;

import java.util.Map;


/**
 * @author Stephane Nicoll
 */
public class StepExecutionRequestTestListener extends MessageListenerAdapter {

    /**
     * The key holding the time the listener should wait, in ms.
     */
    public static final String FAKE_PROCESSING_TIME_KEY = "fakeProcessingTime";

    /**
     * The key specifying if a reply should be sent.
     */
    public static final String SEND_REPLY_KEY = "reply";

    private final Log logger = LogFactory.getLog(StepExecutionRequestTestListener.class);

    private final MessageConverter messageConverter = new SimpleMessageConverter();


    @Override
    public void onMessage(Message message, Channel channel) throws Exception {
        final StepExecutionResult result = execute(message);
        if (result != null) {
            handleResult(result, message, channel);
        }
    }

    public StepExecutionResult execute(Message message) {
        logger.info("Received message");
        final StepExecutionRequest request = (StepExecutionRequest) messageConverter.fromMessage(message);

        final Map<String, Object> headers = message.getMessageProperties().getHeaders();
        if (headers.containsKey(FAKE_PROCESSING_TIME_KEY)) {
            final long waitMs = (Long) headers.get(FAKE_PROCESSING_TIME_KEY);
            logger.debug("Simulating processing time, waiting [" + waitMs + "ms]");
            try {
                Thread.sleep(waitMs);
            } catch (InterruptedException e) {
                throw new IllegalStateException("Message processing interrupted while sleeping");
            }
        }

        // Handle reply
        if (headers.containsKey(SEND_REPLY_KEY)) {
            logger.debug("Sending reply.");
            return new StepExecutionResult(request);
        } else {
            return null;
        }
    }
}
