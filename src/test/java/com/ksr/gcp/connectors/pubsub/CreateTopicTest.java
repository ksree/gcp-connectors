package com.ksr.gcp.connectors.pubsub;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Before;
import org.junit.Test;

public class CreateTopicTest {
    Config conf;


    @Before
    public void setUp()  {
        conf = ConfigFactory.load();
    }

    @Test
    public void createTopic() {
        CreateTopic.createTopic(conf);
    }

}
