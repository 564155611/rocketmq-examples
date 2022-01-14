package com.fanx.rocketmq.examples.filter.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

public class Producer {
    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException, UnsupportedEncodingException {
        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");
        producer.start();
        String tag = "tag";
        for (int i = 0; i < 100; i++) {
            Message msg = new Message("TopicTest",tag,("Hello RocketMQ "+i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            msg.putUserProperty("a", String.valueOf(i));
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
        }
        producer.shutdown();
    }
}
