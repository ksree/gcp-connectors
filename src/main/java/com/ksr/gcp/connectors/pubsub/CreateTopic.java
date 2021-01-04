package com.ksr.gcp.connectors.pubsub;

import java.io.IOException;

import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;
import com.ksr.Action;
import com.typesafe.config.Config;

import java.io.IOException;

public class CreateTopic implements Action {

    public static Topic createTopic(Config config)  {
        String projectId = config.getString("pubsub.projectId");
        String topicId = config.getString("pubsub.topicId");
        return createTopic(projectId, topicId);
    }

    public static Topic createTopic(String projectId, String topicId)  {
        Topic topic = null;
        try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
            TopicName topicName = TopicName.of(projectId, topicId);
            topic = topicAdminClient.createTopic(topicName);
            System.out.println("Created topic: " + topic.getName());
            return topic;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return topic;
    }

    @Override
    public void execute(Config conf) {
        CreateTopic.createTopic(conf);
    }
}
