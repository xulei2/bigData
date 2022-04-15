package com.xl.test;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;


public class SparkTest {
    public static void main(String[] args) {
        try {
            SparkConf conf = new SparkConf()
                    //本地运行
                    .setMaster("local[2]")
                    .setAppName("SparkTest")
                    .set("spark.streaming.kafka.maxRatePerPartition","100")
                    .set("spark.dynamicAllocation.enabled","false")
                    .set("spark.driver.extraJavaOptions","-Dlog4j.configuration=log4j.properties -XX:+UseG1GC")
                    .set("spark.executor.extraJavaOptions","-Dlog4j.configuration=log4j.properties -XX:+UseG1GC")
                    .set("spark.streaming.backpressure.enabled","true")
                    .set("spark.streaming.backpressure.initialRate","100")
                    .set("spark.rdd.compress","true")
                    .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                    .set("spark.shuffle.consolidateFiles","true")
                    .set("spark.driver.memory","1G")
                    .set("spark.executor.instances","4")
                    .set("spark.executor.cores","8")
                    .set("spark.executor.memory","1G")
                    .registerKryoClasses(new Class[]{});
                    conf.validateSettings();
            JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10L));

            HashMap<String, Object> kafkaParams = new HashMap<>(8);
            kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"81.68.158.136:9092");
            kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG,"SparkTest");
            kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
            kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
            kafkaParams.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,"600000");
            kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
            JavaInputDStream<ConsumerRecord<Object, Object>> kafkaDstream = KafkaUtils.createDirectStream(
                    jssc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.Subscribe(Arrays.asList("testtopic"), kafkaParams)
            );
            kafkaDstream.foreachRDD(rdd ->{
                OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                rdd.repartition(2)
                        .flatMap(SparkTest::flatMap)
                        .foreachPartition(SparkTest::foreachPartition);
                ((CanCommitOffsets)kafkaDstream.inputDStream()).commitAsync(offsetRanges);
            });
            jssc.start();
            jssc.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Iterator<String> flatMap(ConsumerRecord c){
        ArrayList<String> list = new ArrayList<>();
        list.add(c.value().toString());
        return list.listIterator();
    }

    private static void foreachPartition(Iterator<String> it){
        if (it.hasNext()){
            String json = it.next();
            System.out.println(json);
        }

    }


}
