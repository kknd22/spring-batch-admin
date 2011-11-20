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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

/**
 * @author Stephane Nicoll
 */
public class CountBasedAggregationCompletionPolicyTests {

    @Test
    public void createWitZeroCount() {
        try {
            new CountBasedAggregationCompletionPolicy(0);
            fail("Should have failed to create instance with 0 expected items");
        } catch (IllegalStateException e) {
            // OK
        }
    }

    @Test
    public void createWitNegativeCount() {
        try {
            new CountBasedAggregationCompletionPolicy(-4);
            fail("Should have failed to create instance with negative expected items");
        } catch (IllegalStateException e) {
            // OK
        }
    }

    @Test
    public void policyMatchesCount() {
        final CountBasedAggregationCompletionPolicy policy = new CountBasedAggregationCompletionPolicy(2);
        shouldComplete(policy, 0, false);
        policy.onItemRegistration("first item");
        shouldComplete(policy, 1, false);
        policy.onItemRegistration("second item");
        shouldComplete(policy, 2, true);
    }

    protected void shouldComplete(CountBasedAggregationCompletionPolicy policy, int actualCount, boolean completeExpected) {
        assertEquals("Wrong state: expected [" + policy.getCount() + "] and got [" + actualCount + "]",
                completeExpected, policy.shouldComplete());
    }

}
