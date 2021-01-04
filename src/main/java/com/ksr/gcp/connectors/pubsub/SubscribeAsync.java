package com.ksr.gcp.connectors.pubsub;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.ksr.util.Util;
import com.typesafe.config.Config;
import io.grpc.netty.shaded.io.netty.handler.codec.http.multipart.FileUpload;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SubscribeAsync {
    String projectId;
    String topicId;
    String subscriptionId;
    Schema schema;

    private static void serialize(final Schema schema, final GenericRecord... users) throws IOException {
        // Serialize users to disk
        final DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(writer)) {
            fileWriter.create(schema, new File(""));
            for (GenericRecord user : users) {
                fileWriter.append(user);
            }
        }
    }
    public SubscribeAsync(Config config) throws IOException {
        this.projectId = config.getString("pubsub.projectId");
        this.topicId = config.getString("pubsub.topicId");
        this.subscriptionId = config.getString("pubsub.subscriptionId");
        //String schema = config.getString("pubsub.avroSchema");
        try {
            File schemaFile = new Util().getFileFromResource(config.getString("pubsub.avroSchema"));
            schema = new Schema.Parser().parse(schemaFile);
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

    }

    public  void writeToAvro() {
        ProjectSubscriptionName subscriptionName =
                ProjectSubscriptionName.of(projectId, subscriptionId);
        DatumReader<GenericData.Record> datumReader = new GenericDatumReader<>(schema);

        // Instantiate an asynchronous message receiver.
        MessageReceiver receiver =
                (PubsubMessage message, AckReplyConsumer consumer) -> {
                    // Handle incoming message, then ack the received message.
                    System.out.println("Received message Id: " + message.getMessageId());
                    SeekableInput data = new SeekableByteArrayInput(message.getData().toByteArray());

                    try {
                        DataFileReader<GenericData.Record> genericRecords = new DataFileReader<>(data, datumReader);

                        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
                        DataFileWriter<GenericRecord> writer = new DataFileWriter<GenericRecord>(datumWriter);
                        File avroDataFile = new File("target/generated-sources/employee_nongen.avro");

                        writer.create(schema, avroDataFile);
                        while(genericRecords.hasNext()){
                            writer.append(genericRecords.next());
                        }
                        writer.flush();
                        writer.close();
                    } catch (IOException e) {
                        System.out.println("Failed to parse Avro message + Id: " + message.getMessageId());
                        e.printStackTrace();
                    }

                    consumer.ack();
                };

        Subscriber subscriber = null;
        try {
            subscriber = Subscriber.newBuilder(subscriptionName, receiver).build();
            // Start the subscriber.
            subscriber.startAsync().awaitRunning();
            System.out.printf("Listening for messages on %s:\n", subscriptionName.toString());
            // Allow the subscriber to run for 30s unless an unrecoverable error occurs.
            subscriber.awaitTerminated(30, TimeUnit.SECONDS);
        } catch (TimeoutException timeoutException) {
            // Shut down the subscriber after 30s. Stop receiving messages.
            subscriber.stopAsync();
        }
    }
}