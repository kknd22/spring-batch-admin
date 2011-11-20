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
package org.springframework.batch.execution.aggregation.jms;

import org.springframework.batch.execution.aggregation.core.support.BaseAggregationContext;

import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.Session;

/**
 * The default {@link JmsAggregationContext} implementation.
 *
 * @author Stephane Nicoll
 */
class JmsAggregationContextImpl<T> extends BaseAggregationContext<Message, T>
        implements JmsAggregationContext<T> {

    private Session session;
    private Destination destination;
    private long receiveTimeout;

    public Session getSession() {
        return session;
    }

    void setSession(Session session) {
        this.session = session;
    }

    public Destination getDestination() {
        return destination;
    }

    void setDestination(Destination destination) {
        this.destination = destination;
    }

    public long getReceiveTimeout() {
        return receiveTimeout;
    }

    void setReceiveTimeout(long receiveTimeout) {
        this.receiveTimeout = receiveTimeout;
    }

}
