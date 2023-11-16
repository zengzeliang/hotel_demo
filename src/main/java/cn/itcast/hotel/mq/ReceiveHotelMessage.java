package cn.itcast.hotel.mq;

import cn.itcast.hotel.config.RocketMqConfig;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * @ClassName ResiveHotelMessgae
 * @Description TODO
 * @Author admin
 * Date 2023/11/16 10:04 上午
 * Version 1.0
 **/
@Component
@Slf4j
public class ReceiveHotelMessage {

    @Autowired
    private RocketMqConfig rocketMqConfig;

    @Autowired
    private IHotelService hotelService;

    @Autowired
    private RestHighLevelClient client;

    @Bean
    public DefaultMQPushConsumer receiveInsertHotelMessage() throws MQClientException {
        // 实例化消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(rocketMqConfig.getConsumerGroup());

        // 设置NameServer的地址
        consumer.setNamesrvAddr(rocketMqConfig.getNameServer());

        // 订阅一个或者多个Topic，以及Tag来过滤需要消费的消息
        consumer.subscribe(rocketMqConfig.getTopic(), "*");
        // 注册回调实现类来处理从broker拉取回来的消息
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                try {
                    String message = new String(msg.getBody());
                    if(msg.getTags().equals(rocketMqConfig.getInsertTag())){
                        Hotel hotel = hotelService.getById(message);
                        if(hotel != null){
                            HotelDoc hotelDoc = new HotelDoc(hotel);
                            IndexRequest request = new IndexRequest("hotel").id(hotel.getId().toString());
                            request.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
                            client.index(request, RequestOptions.DEFAULT);
                        }
                        System.out.println(String.format("received insert message: messageId: %s, body: %s", msg.getMsgId(), new String(msg.getBody())));
                    }else if(msg.getTags().equals(rocketMqConfig.getDeleteTag())){
                        DeleteRequest request = new DeleteRequest("hotel", message);
                        client.delete(request, RequestOptions.DEFAULT);
                        System.out.println(String.format("received delete message: messageId: %s, body: %s", msg.getMsgId(), new String(msg.getBody())));
                    }
                } catch (IOException e) {
                    log.info("发送es失败");
                }
            }
            // 标记该消息已经被成功消费
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        // 启动消费者实例
        consumer.start();
        return consumer;
    }

}
