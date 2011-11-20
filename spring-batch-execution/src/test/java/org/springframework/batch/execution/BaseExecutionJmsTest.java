package org.springframework.batch.execution;

import org.apache.activemq.command.ActiveMQQueue;
import org.junit.runner.RunWith;
import org.springframework.batch.execution.support.jms.ExtendedJmsTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.jms.Queue;

/**
 * @author Stephane Nicoll
 */
@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext
public abstract class BaseExecutionJmsTest {

    protected final Queue queueA = new ActiveMQQueue("queueA");

    @Autowired
    protected ExtendedJmsTemplate extendedJmsTemplate;
}

