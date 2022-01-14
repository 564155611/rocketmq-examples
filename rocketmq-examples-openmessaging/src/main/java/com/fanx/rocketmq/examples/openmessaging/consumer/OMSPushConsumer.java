package com.fanx.rocketmq.examples.openmessaging.consumer;

import io.openmessaging.Message;
import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.OMS;
import io.openmessaging.OMSBuiltinKeys;
import io.openmessaging.consumer.MessageListener;
import io.openmessaging.consumer.PushConsumer;
import io.openmessaging.producer.Producer;

public class OMSPushConsumer {
    public static void main(String[] args) {
        final MessagingAccessPoint messagingAccessPoint =
                OMS.getMessagingAccessPoint("oms:rocketmq://localhost:9876/default:default");
        final Producer producer = messagingAccessPoint.createProducer();
        final PushConsumer consumer =
                messagingAccessPoint.createPushConsumer(OMS.newKeyValue()
                        .put(OMSBuiltinKeys.CONSUMER_ID,
                                "OMS_CONSUMER"));
        messagingAccessPoint.startup();
        System.out.println("MessagingAccessPoint startup OK");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.shutdown();
            messagingAccessPoint.shutdown();
        }));
        consumer.attachQueue("OMS_HELLO_TOPIC", new MessageListener() {
            @Override
            public void onReceived(Message message, Context context) {
                System.out.println("Received one message: " + message.sysHeaders().getString(Message.BuiltinKeys.MESSAGE_ID));
                context.ack();
            }
        });
        consumer.startup();
        System.out.println("Consumer startup OK");

    }

}
