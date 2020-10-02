package com.redhat.events;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.StaticHandler;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.mirror.RemoteClusterUtils;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

public class EventAnalysisVerticle extends AbstractVerticle {
    

    private static final AtomicInteger COUNTER = new AtomicInteger();



    private void init() {

        Map<String, Object> config = new HashMap();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "operational-cluster-kafka-bootstrap:9092");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        config.put("key.serializer", StringSerializer.class);
        config.put("value.serializer", StringSerializer.class);
        config.put("group.id", "another_grp");
        config.put("auto.offset.reset", "earliest");
        config.put("enable.auto.commit", "true");

        org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(config);

        ConsumerRebalanceListener offsetHandler = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                List<Map<TopicPartition, OffsetAndMetadata>> mirroredOffsets = getMirroredOffsets();

                for (TopicPartition partition : partitions) {
                    OffsetAndMetadata mirroredOffset = findHighestMirroredOffset(mirroredOffsets, partition);
                    OffsetAndMetadata localOffset = consumer.committed(Collections.singleton(partition)).get(partition);

                    if (mirroredOffset != null) {
                        if (localOffset != null)    {
                            if (mirroredOffset.offset() > localOffset.offset()) {
                                System.out.println("Seeking to {} in {} (higher than local offset {})"+mirroredOffset.offset()+partition+localOffset.offset());
                                consumer.seek(partition, mirroredOffset);
                            } else {
                                System.out.println("Keeping local offset {} in {} (higher than mirrored offset {})"+localOffset.offset()+partition+ mirroredOffset.offset());
                            }
                        } else {
                            System.out.println("Seeking to {} in {} (local offset does not exist)"+ mirroredOffset.offset()+partition);
                            consumer.seek(partition, mirroredOffset);
                        }
                    } else {
                        System.out.println("Mirrored offset does not exist for partition {}"+partition);
                    }
                }
            }

            public OffsetAndMetadata findHighestMirroredOffset(List<Map<TopicPartition, OffsetAndMetadata>> mirroredOffsets, TopicPartition partition) {
                OffsetAndMetadata foundOffset = null;

                for (Map<TopicPartition, OffsetAndMetadata> offsets : mirroredOffsets)  {
                    if (offsets.containsKey(partition)) {
                        if (foundOffset == null)    {
                            foundOffset = offsets.get(partition);
                        } else  {
                            OffsetAndMetadata newOffset = offsets.get(partition);
                            if (foundOffset.offset() < newOffset.offset())   {
                                foundOffset = newOffset;
                            }
                        }
                    }
                }

                return foundOffset;
            }

            public List<Map<TopicPartition, OffsetAndMetadata>> getMirroredOffsets() {
                Set<String> clusters = null;
                try {
                    clusters = RemoteClusterUtils.upstreamClusters(config);
                } catch (InterruptedException e) {
                    System.out.println("Failed to get remote cluster"+ e);
                    return Collections.emptyList();
                } catch (TimeoutException e) {
                    System.out.println("Failed to get remote cluster"+ e);
                    return Collections.emptyList();
                }

                List<Map<TopicPartition, OffsetAndMetadata>> mirroredOffsets = new ArrayList<>();

                for (String cluster : clusters) {
                    try {
                        mirroredOffsets.add(RemoteClusterUtils.translateOffsets(config, cluster, config.get(ConsumerConfig.GROUP_ID_CONFIG).toString(), Duration.ofMinutes(1)));
                    } catch (InterruptedException e) {
                       System.out.println("Failed to translate offsets"+ e);
                        e.printStackTrace();
                    } catch (TimeoutException e) {
                        System.out.println("Failed to translate offsets"+ e);
                    }
                }

                return mirroredOffsets;
            }
        };


        consumer.subscribe(Pattern.compile(".*transaction-whitelist"), offsetHandler);

        int messageNo = 0;

        while (true)
        {
            ConsumerRecords<String, String> records = consumer.poll(60000);

            if(records.isEmpty()) {
               System.out.println("No message in topic for {} seconds. Finishing ..."+60000/1000);
                break;
            }

            for (ConsumerRecord<String, String> record : records)
            {
               System.out.println("Received message no. {}: {} / {} (from topic {}, partition {}, offset {})"+ ++messageNo+ record.key()+ record.value()+ record.topic()+ record.partition()+ record.offset());
            }

            consumer.commitSync();
        }

        consumer.close();


    }

    @Override
    public void start(Future<Void> fut) {

        WebClientOptions options = new WebClientOptions();
        options.setKeepAlive(false);
        WebClient client = WebClient.create(vertx, options);

        init();

        Router router = Router.router(vertx);

        router.route().handler(BodyHandler.create());


        router.route("/static/*").handler(StaticHandler.create("assets"));


        vertx.createHttpServer().requestHandler(router::accept).listen(config().getInteger("http.port", 8080), result -> {
            if (result.succeeded()) {
                fut.complete();
            } else {
                fut.fail(result.cause());
            }
        });
    }




}
