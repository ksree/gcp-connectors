package com.ksr.gcp.connectors.pubsub

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.junit.Test


class CreateTopicTest {
    Config conf = ConfigFactory.load();

    @Test
    void testCreateTopic() {
        assert(CreateTopic.createTopic(conf).name == conf.getString("pubSub.topicId"));
    }
}
