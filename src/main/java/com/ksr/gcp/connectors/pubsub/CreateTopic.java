package com.ksr.gcp.connectors.pubsub;

import java.io.IOException;

import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;
import com.typesafe.config.Config;

import java.io.IOException;

public class CreateTopic {

    public static void createTopic(Config config) throws IOException {
        String projectId = config.getString("gcs.projectId");
        String topicId = config.getString("gcs.topicId");

        try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
            TopicName topicName = TopicName.of(projectId, topicId);
            Topic topic = topicAdminClient.createTopic(topicName);
            System.out.println("Created topic: " + topic.getName());
        }
    }
}
