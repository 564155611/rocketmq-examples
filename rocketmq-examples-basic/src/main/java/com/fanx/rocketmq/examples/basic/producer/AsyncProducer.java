package com.fanx.rocketmq.examples.basic.producer;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.CountDownLatch2;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.TimeUnit;

/**
 * 适合对响应时间敏感的业务场景,即发送端不能长时间等待Broker的响应
 */
public class AsyncProducer {
    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException {
        // 实例化消息生产者
        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");
        // 设置NameServer的地址
        producer.setNamesrvAddr("localhost:9876");
        // 启动Producer实例
        producer.start();

        producer.setRetryTimesWhenSendAsyncFailed(0);

        int messageCount = 100;
        final CountDownLatch2 countDownLatch = new CountDownLatch2(messageCount);
        for (int i = 0; i < messageCount; i++) {
            final int index = i;
            // 创建消息,并指定topic, tag和消息体
            Message msg = new Message("TopicTest",
                    "TagA",
                    ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            // SendCallback接收异步返回结果的回调
            producer.send(msg, new SendCallback() {

                @Override
                public void onSuccess(SendResult sendResult) {
                    countDownLatch.countDown();
                    System.out.printf("%-10d OK %s %n", index, sendResult.getMsgId());
                }

                @Override
                public void onException(Throwable e) {
                    countDownLatch.countDown();
                    System.out.printf("%-10d Exception %s %n", index, e);
                    e.printStackTrace();
                }
            });
        }
        // 等待5s
        countDownLatch.await(5, TimeUnit.SECONDS);
        // 如果不再发送消息,关闭Producer实例
        producer.shutdown();
    }
}
