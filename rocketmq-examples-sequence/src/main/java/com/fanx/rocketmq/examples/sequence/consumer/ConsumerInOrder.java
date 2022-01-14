package com.fanx.rocketmq.examples.sequence.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class ConsumerInOrder {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("please_rename_unique_group_name_3");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        /*
        * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费
        * 如果非第一次启动,那么按照上次消费的位置继续消费
        * */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        consumer.subscribe("TopicTest", "TagA||TagC||TagD");

        consumer.registerMessageListener(new MessageListenerOrderly(){
            Random random = new Random();

            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                context.setAutoCommit(true);

                for (MessageExt msg : msgs) {
                    // 可以看到每个Queue有唯一的consume线程来消费,订单对每个queue而言是有序的
                    System.out.printf("consumeThread=%s queueId=%d, content:%s%n",
                            Thread.currentThread().getName(),
                            msg.getQueueId(),
                            new String(msg.getBody()));
                }

                try {
                    // 模拟业务逻辑处理
                    TimeUnit.SECONDS.sleep(random.nextInt(10));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        consumer.start();
        System.out.println("Consumer Started.");
    }
}
