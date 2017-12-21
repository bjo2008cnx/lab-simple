package com.github.example.kafka;

import java.util.Arrays;
import java.util.List;

/**
 * KafkaConstant
 *
 * @author Michael.Wang
 * @date 2017/12/21
 */
public class KafkaConstant {
    public static final String URL = "localhost:9092";
    public static final List<String> TOPICS = Arrays.asList("my-replicated-topic", "bar");
}