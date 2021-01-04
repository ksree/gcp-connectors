package com.ksr.gcp.connectors.pubsub;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class SubscribeAsyncTest {

    SubscribeAsync subscribeAsync;
    Config conf = ConfigFactory.load();

    @Before
    public void setUp() throws Exception {
        subscribeAsync = new SubscribeAsync(conf);
    }


    @Test
    public void writeToAvro(){
        subscribeAsync.writeToAvro();
    }
}