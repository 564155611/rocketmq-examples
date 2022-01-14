package com.fanx.rocketmq.examples.openmessaging.consumer;

import io.openmessaging.Message;
import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.OMS;
import io.openmessaging.OMSBuiltinKeys;
import io.openmessaging.consumer.PullConsumer;
import io.openmessaging.producer.Producer;
import io.openmessaging.producer.SendResult;

public class OMSPullConsumer {
    public static void main(String[] args) {
        final MessagingAccessPoint messagingAccessPoint =
                OMS.getMessagingAccessPoint("oms:rocketmq://localhost:9876/default:default");
        final Producer producer = messagingAccessPoint.createProducer();
        final PullConsumer consumer =
                messagingAccessPoint.createPullConsumer(OMS.newKeyValue()
                                .put(OMSBuiltinKeys.CONSUMER_ID,
                                        "OMS_CONSUMER"));
        messagingAccessPoint.startup();
        System.out.println("MessagingAccessPoint startup OK");

        final String queueName = "TopicTest";
        producer.startup();
        Message msg = producer.createBytesMessage(queueName, "Hello Open Messaging".getBytes());
        SendResult sendResult = producer.send(msg);
        System.out.printf("Send Message OK. msgId: %s%n", sendResult.messageId());
        producer.shutdown();
        consumer.attachQueue(queueName);
        consumer.startup();
        System.out.println("Consumer startup OK");
        //运行直到一个消息被发送了
        boolean stop = false;
        while (!stop) {
            Message message = consumer.receive();
            if (message != null) {
                String msgId = message.sysHeaders().getString(Message.BuiltinKeys.MESSAGE_ID);
                System.out.printf("Receive one message: %s%n", msgId);
                consumer.ack(msgId);
                stop = msgId.equalsIgnoreCase(sendResult.messageId());
            }else {
                System.out.println("Return without any message");
            }
        }
        consumer.shutdown();
        messagingAccessPoint.shutdown();
    }
}
