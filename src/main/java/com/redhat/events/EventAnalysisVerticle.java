package com.redhat.events;

import com.Event;
import com.eventAnalysis;
import com.google.gson.Gson;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class EventAnalysisVerticle extends AbstractVerticle {
    

    private static final AtomicInteger COUNTER = new AtomicInteger();


    private WebClient client;
    private eventAnalysis eventAnalysis;
    private Set<eventAnalysis> offersHome = new HashSet<>();
    private Set<eventAnalysis> paymentsHome = new HashSet<>();

    private List<Event> customer1EventList = new ArrayList<>();
    private Set<AlertMapObj> alertMap = new HashSet<>();



    private Event event;


    private void init() {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-cluster-kafka-broker:9092");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        config.put("key.serializer", StringSerializer.class);
        config.put("value.serializer", StringSerializer.class);
        config.put("group.id", "another_grp");
        config.put("auto.offset.reset", "earliest");
        config.put("enable.auto.commit", "false");

        KafkaConsumer<String, String> customerEvents = getKafkaConsumerCustomerEvents(config,vertx);

        customerEvents.handler(record -> {
           
            Map<String, Object> value = new Gson().fromJson(record.value(),Map.class);

 

                    event = new Gson().fromJson(new Gson().toJson(value), com.Event.class);


            System.out.print("Event"+ event);
            if(null != event && null != event.getEventCategory()) {
                customer1EventList.add(event);

            }
        });


        // use consumer for interacting with Apache Kafka
        KafkaConsumer<String, String> consumer = getKafkaConsumer(config,vertx);



        consumer.handler(record -> {

            if(record.key().equalsIgnoreCase("\"John\"")) {


                Map<String, Object> value = new Gson().fromJson(record.value(),Map.class);

                for(String key:value.keySet()) {
                    if(key.equals("value")) {

                        eventAnalysis = new Gson().fromJson(new Gson().toJson(value.get(key)), com.eventAnalysis.class);
                    }
                }


                if(null != eventAnalysis && null != eventAnalysis.getEventResponsePayload()) {
                    offersHome.add(eventAnalysis);

                }

            }

            if(record.key().equalsIgnoreCase("\"James\"")) {




                Map<String, Object> value = new Gson().fromJson(record.value(),Map.class);

                for(String key:value.keySet()) {
                    if(key.equals("value")) {

                        eventAnalysis = new Gson().fromJson(new Gson().toJson(value.get(key)), com.eventAnalysis.class);
                    }
                }


                if(null != eventAnalysis && null != eventAnalysis.getEventResponsePayload()) {
                    paymentsHome.add(eventAnalysis);
                }

            }


        });

        KafkaConsumer<String, String> atmEvents = getFraudPatternStream(config,vertx);
        alertMap = new HashSet<>();

        atmEvents.handler(record -> {
           AlertMapObj  alertMapObj = new AlertMapObj();
           alertMapObj.setUserId(record.key().replaceAll("\"",""));

           alertMapObj.setNoOfAttempts(record.value());
           alertMap.add(alertMapObj);

        });

        System.out.println("Alert Map:"+alertMap);


    }

    @Override
    public void start(Future<Void> fut) {

        WebClientOptions options = new WebClientOptions();
        options.setKeepAlive(false);
        client = WebClient.create(vertx, options);

        init();

        Router router = Router.router(vertx);

        router.route().handler(BodyHandler.create());

        router.post("/homePost").handler(this::postHome);

        router.post("/paymentPost").handler(this::postPayments);
        router.post("/offerStream").handler(this::offerStream);

        router.post("/eventStream").handler(this::eventStream);

        router.post("/alertMap").handler(this::alertMap);



        router.route("/static/*").handler(StaticHandler.create("assets"));


        vertx.createHttpServer().requestHandler(router::accept).listen(config().getInteger("http.port", 8080), result -> {
            if (result.succeeded()) {
                fut.complete();
            } else {
                fut.fail(result.cause());
            }
        });
    }




    private void postHome(RoutingContext routingContext) {




        if(null != offersHome && !offersHome.isEmpty()) {
            routingContext.response().setStatusCode(201).putHeader("content-type", "application/json")
                    .end(Json.encodePrettily(new Gson().toJson(offersHome)));
        }



    }

    private void alertMap(RoutingContext routingContext) {




        if(null != alertMap && !alertMap.isEmpty()) {
            routingContext.response().setStatusCode(201).putHeader("content-type", "application/json")
                    .end(Json.encodePrettily(new Gson().toJson(alertMap)));
        }



    }

    private void offerStream(RoutingContext routingContext) {




        if(null != offersHome && !offersHome.isEmpty()) {
            routingContext.response().setStatusCode(201).putHeader("content-type", "application/json")
                    .end(Json.encodePrettily(new Gson().toJson(offersHome)));
        }



    }

    private void eventStream(RoutingContext routingContext) {




        if(null != customer1EventList && !customer1EventList.isEmpty()) {
            routingContext.response().setStatusCode(201).putHeader("content-type", "application/json")
                    .end(Json.encodePrettily(new Gson().toJson(customer1EventList)));
        }



    }

    private void postPayments(RoutingContext routingContext) {


        if(null != paymentsHome && !paymentsHome.isEmpty()) {
            routingContext.response().setStatusCode(201).putHeader("content-type", "application/json")
                    .end(Json.encodePrettily(new Gson().toJson(paymentsHome)));
        }

    }

    private static KafkaConsumer<String, String> getKafkaConsumer(Properties config, Vertx vertx) {
        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, config);


        // subscribe to several topics
        Set<String> topics = new HashSet<>();
        topics.add("offer-output-stream");

        consumer.subscribe(topics);
        return consumer;
    }

    private static KafkaConsumer<String, String> getKafkaConsumerCustomerEvents(Properties config, Vertx vertx) {
        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, config);


        // subscribe to several topics
        Set<String> topics = new HashSet<>();
        topics.add("event-input-stream");

        consumer.subscribe(topics);
        return consumer;
    }

    private static KafkaConsumer<String, String> getFraudPatternStream(Properties config, Vertx vertx) {
        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, config);


        // subscribe to several topics
        Set<String> topics = new HashSet<>();
        topics.add("ATM_Response");

        consumer.subscribe(topics);
        return consumer;
    }

}
