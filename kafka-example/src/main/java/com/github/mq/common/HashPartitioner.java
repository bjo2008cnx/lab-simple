package com.github.mq.common;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

/**
 * 实现HashPartitioner从而保证key相同的消息被发送到同一个Partition
 * example:
 * props.put("partitioner.class", HashPartitioner.class.getName());
 * props.put("interceptor.classes", EvenProducerInterceptor.class.getName());
 */
public class HashPartitioner implements Partitioner {

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        if (keyBytes != null) {
            int hashCode = 0;
            if (key instanceof Integer || key instanceof Long) {
                hashCode = (int) key;
            } else {
                hashCode = key.hashCode();
            }
            hashCode = hashCode & 0x7fffffff;
            return hashCode % numPartitions;
        } else {
            return 0;
        }
    }

    @Override
    public void close() {
    }

}
