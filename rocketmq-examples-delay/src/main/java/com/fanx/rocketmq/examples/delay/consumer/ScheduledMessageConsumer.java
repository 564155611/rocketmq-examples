package com.fanx.rocketmq.examples.delay.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class ScheduledMessageConsumer {
    public static void main(String[] args) throws MQClientException {
        // 实例化消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("ExampleConsumer");
        // 订阅Topics
        consumer.subscribe("TestTopic", "*");
        // 注册消息监听者
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messages, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for (MessageExt message : messages) {
                    System.out.println("Receive message[msgId="+message.getMsgId()+"] "+(System.currentTimeMillis()-message.getBornTimestamp())+"ms later");
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 启动消费者
        consumer.start();
    }

}
