package com.ksr.gcp.connectors.pubsub;

import com.ksr.avro.datagen.DataGen;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class PublishMessageTest {

    PublishMessage publishMessage;
    Config conf = ConfigFactory.load();
    DataGen dataGen;

    @Before
    public void setUp() throws Exception {
        publishMessage = new PublishMessage(conf);
        dataGen = new DataGen();
    }

    @Test
    public void publish() throws IOException, InterruptedException {
        publishMessage.publish(dataGen.getSerializedRecord());
        publishMessage.publish(dataGen.getSerializedRecord());
        publishMessage.publish(dataGen.getSerializedRecord());
    }
}