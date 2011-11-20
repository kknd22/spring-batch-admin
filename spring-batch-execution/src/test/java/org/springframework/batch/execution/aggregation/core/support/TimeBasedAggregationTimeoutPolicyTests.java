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
package org.springframework.batch.execution.aggregation.core.support;

import org.junit.Test;
import org.springframework.batch.execution.aggregation.core.AggregationTimeoutPolicy;

import static junit.framework.Assert.*;

/**
 * @author Stephane Nicoll
 */
public class TimeBasedAggregationTimeoutPolicyTests {

    @Test
    public void createWithZeroMs() {
        try {
            new TimeBasedAggregationTimeoutPolicy(0);
            fail("Should have failed to create an instance with 0 ms");
        } catch (IllegalStateException e) {
            // OK
        }
    }

    @Test
    public void createWithNegativeMs() {
        try {
            new TimeBasedAggregationTimeoutPolicy(-5);
            fail("Should have failed to create an instance with -5 ms");
        } catch (IllegalStateException e) {
            // OK
        }
    }

    @Test
    public void wait50Ms() {
        final AggregationTimeoutPolicy policy = new TimeBasedAggregationTimeoutPolicy(25);
        final long startTime = System.currentTimeMillis();
        assertFalse("Should not have timeout", policy.shouldTimeout(startTime));
        try {
            Thread.sleep(30);
        } catch (InterruptedException e) {
            throw new IllegalStateException("Interrupted, should not happen");
        }
        assertTrue("Should have timeout, 25 ms have been elapsed since the beginning", policy.shouldTimeout(startTime));
    }

}
