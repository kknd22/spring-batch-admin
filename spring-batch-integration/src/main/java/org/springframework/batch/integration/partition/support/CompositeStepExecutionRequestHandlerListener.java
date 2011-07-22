package org.springframework.batch.integration.partition.support;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.integration.partition.StepExecutionRequestHandlerListener;

import java.util.ArrayList;
import java.util.List;

/**
 * Listener composed of a set of other listeners.
 *
 * @author Sebastien Gerard
 */
public class CompositeStepExecutionRequestHandlerListener implements StepExecutionRequestHandlerListener {

    private final Log logger = LogFactory.getLog(CompositeStepExecutionRequestHandlerListener.class);
    private final List<StepExecutionRequestHandlerListener> handlerListeners;

    /**
     * Initializes the listener with an empty listener set.
     */
    public CompositeStepExecutionRequestHandlerListener() {
        this(new ArrayList<StepExecutionRequestHandlerListener>());
    }

    /**
     * Initializes the listener with the given listener set.
     *
     * @param handlerListeners the listener set
     */
    public CompositeStepExecutionRequestHandlerListener(List<StepExecutionRequestHandlerListener> handlerListeners) {
        this.handlerListeners = handlerListeners;
    }

    public void beforeHandle(StepExecution stepExecution) {
        for (StepExecutionRequestHandlerListener listener : handlerListeners) {
            try {
                listener.beforeHandle(stepExecution);
            } catch (Exception e) {
                logger.warn("Unexpected exception in a request handler listener", e);
            }
        }
    }

    public void onDeniedException(StepExecution stepExecution) {
        for (StepExecutionRequestHandlerListener listener : handlerListeners) {
            try {
                listener.onDeniedException(stepExecution);
            } catch (Exception e) {
                logger.warn("Unexpected exception in a request handler listener", e);
            }
        }
    }

    public void afterHandle(StepExecution stepExecution) {
        for (StepExecutionRequestHandlerListener listener : handlerListeners) {
            try {
                listener.afterHandle(stepExecution);
            } catch (Exception e) {
                logger.warn("Unexpected exception in a request handler listener", e);
            }
        }
    }

}

