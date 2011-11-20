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
package org.springframework.batch.execution.aggregation.core;

/**
 * Listener interface used to track item registration.
 *
 * @author Stephane Nicoll
 */
public interface AggregationItemListener<T> {

    /**
     * Invoked when a new item has been received by the aggregation service.
     *
     * @param item the registered item
     */
    void onItemRegistration(T item);

}
