package com.fanx.rocketmq.examples.openmessaging.producer;


import io.openmessaging.*;
import io.openmessaging.producer.Producer;
import io.openmessaging.producer.SendResult;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

public class OMSProducer {
    public static void main(String[] args) throws InterruptedException {
        final MessagingAccessPoint messagingAccessPoint =
                OMS.getMessagingAccessPoint("oms:rocketmq://localhost:9876/default:default");
        final Producer producer = messagingAccessPoint.createProducer();
        messagingAccessPoint.startup();
        System.out.println("MessagingAccessPoint startup OK");

        producer.startup();
        System.out.println("Producer startup OK");

        {
            Message message = producer.createBytesMessage("OMS_HELLO_TOPIC", "OMS_HELLO_BODY".getBytes(StandardCharsets.UTF_8));
            SendResult sendResult = producer.send(message);
            System.out.printf("Send async message OK,msgId:%s%n", sendResult.messageId());
        }
        CountDownLatch countDownLatch = new CountDownLatch(1);
        {
            final Future<SendResult> result = producer.sendAsync(producer.createBytesMessage("OMS_HELLO_TOPIC", "OMS_HELLO_BODY".getBytes(StandardCharsets.UTF_8)));
            result.addListener(new FutureListener<SendResult>() {
                @Override
                public void operationComplete(Future<SendResult> future) {
                    if (future.getThrowable() != null) {
                        System.out.printf("Send async message Failed, error: %s%n", future.getThrowable().getMessage());
                    }else {
                        System.out.printf("Send async message OK, msgId: %s%n", future.get().messageId());
                    }
                    countDownLatch.countDown();
                }
            });
        }
        {
            producer.sendOneway(producer.createBytesMessage("OMS_HELLO_TOPIC", "OMS_HELLO_BODY".getBytes(StandardCharsets.UTF_8)));
            System.out.println("Send oneway message OK");
        }
        try {
            countDownLatch.await();
            // 等待一些时间来发送消息
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        producer.shutdown();
    }
}
