package com.fanx.rocketmq.examples.batch.producer;

import com.fanx.rocketmq.examples.batch.producer.util.ListSplitter;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.ArrayList;
import java.util.List;

public class BatchProducer {
    public static void main(String[] args) throws MQClientException {
        DefaultMQProducer producer = new DefaultMQProducer("ExampleBatchProducer");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();

        String topic = "BatchTest";

        List<Message> messages = new ArrayList<>();
        messages.add(new Message(topic, "TagA", "OrderID001", "Hello world 0".getBytes()));
        messages.add(new Message(topic, "TagB", "OrderID002", "Hello world 1".getBytes()));
        messages.add(new Message(topic, "TagC", "OrderID003", "Hello world 2".getBytes()));

        //producer.send(messages);
        ListSplitter splitter = new ListSplitter(messages);
        while (splitter.hasNext()) {
            try {
                List<Message> listItem = splitter.next();
                producer.send(listItem);
            } catch (RemotingException e) {
                e.printStackTrace();
            } catch (MQBrokerException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
