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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.integration.partition.StepExecutionRequest;
import org.springframework.batch.integration.partition.StepExecutionRequestHandler;
import org.springframework.batch.integration.partition.StepExecutionResult;
import org.springframework.beans.factory.annotation.Required;

import java.util.Collections;

/**
 * A simple AMQP message listener used to delegate to the step execution
 * request handler.
 *
 * @author Stephane Nicoll
 * @see StepExecutionRequestHandler
 */
public class StepExecutionRequestMessageListener {

    private final Log logger = LogFactory.getLog(StepExecutionRequestMessageListener.class);

    private StepExecutionRequestHandler stepExecutionRequestHandler;

    public StepExecutionResult process(StepExecutionRequest request) {
        return execute(request);
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
