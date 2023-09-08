package com.github.sumanthvadde;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import com.github.sumanthvadde.dto.*;
import com.github.sumanthvadde.detectors.*;


public class FraudDetectionJob {

    public static void main(String []args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String bootstrapServers = "kafkac:9092";

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
				.setBootstrapServers(bootstrapServers)
				.setTopics("transactions")
				.setGroupId("fraud-detector")
				.setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
                
                DataStream<String> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

            DataStream<Object> transactions = kafkaStream
                .map(CardTransaction::fromString)
                .name("transactions");

                KeyedStream<CardTransaction, String> keyedTransactions = transactions
                .keyBy(CardTransaction::getAccountId);
    
            DataStream<Alert> overLimitAlerts = keyedTransactions
                .process(new OverLimitDetector())
                .name("over-limit-alerts");
    
            DataStream<Alert> smallThenLargeAlerts = keyedTransactions
                .process(new ShadyTransaction())
                .name("small-then-large-alerts");
    
            DataStream<Alert> expiredCardAlerts = keyedTransactions
                .process(new ExpiredCardDetector())
                .name("expired-card-alerts");
    
            DataStream<Alert> normalDistributionAlerts = keyedTransactions
                .process(new NormalDistributionDetector())
                .name("normal-distribution-alerts");
    
            DataStream<Alert> locationAlerts = keyedTransactions
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(60)))
                .process(new LocationDetector())
                    .name("location-alerts");
            

                    DataStream<Alert> alerts = overLimitAlerts
                    .union(smallThenLargeAlerts)
                    .union(expiredCardAlerts)
                    .union(normalDistributionAlerts)
                    .union(locationAlerts);
    
            KafkaSink<String> alertKafkaSink = KafkaSink.<String>builder()
                    .setBootstrapServers(bootstrapServers)
                    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                            .setTopic("alerts")
                            .setValueSerializationSchema(new SimpleStringSchema())
                            .build()
                    ).build();
    
            alerts
                .map(Alert::toJson)
                .sinkTo(alertKafkaSink)
                .name("kafka-alerts");
    
            alerts
                .addSink(new AlertSink())
                .name("send-alerts");
    
            env.execute("Fraud Detection");


    }
    
}
