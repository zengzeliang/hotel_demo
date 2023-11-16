package cn.itcast.hotel.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * @ClassName RocketMqConfig
 * @Description TODO
 * @Author admin
 * Date 2023/11/16 9:25 上午
 * Version 1.0
 **/
@Component
@Data
public class RocketMqConfig {
    @Value("${rocketmq.topic}")
    private String topic;
    @Value("${rocketmq.insertTag}")
    private String insertTag;
    @Value("${rocketmq.deleteTag}")
    private String deleteTag;
    @Value("${rocketmq.name-server}")
    private String nameServer;
    @Value("${rocketmq.consumer.group}")
    private String consumerGroup;
}
