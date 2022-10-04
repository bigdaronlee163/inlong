package org.apache.inlong.tubemq.example;

import org.apache.commons.codec.binary.StringUtils;
import org.apache.inlong.tubemq.client.config.TubeClientConfig;
import org.apache.inlong.tubemq.client.factory.MessageSessionFactory;
import org.apache.inlong.tubemq.client.factory.TubeSingleSessionFactory;
import org.apache.inlong.tubemq.client.producer.MessageProducer;
import org.apache.inlong.tubemq.client.producer.MessageSentResult;
import org.apache.inlong.tubemq.corebase.Message;

public class TestAsyncProducerExample {
    public static void main(String[] args) throws Throwable {
        final String masterHostAndPort = "localhost:8715";
        final TubeClientConfig clientConfig = new TubeClientConfig(masterHostAndPort);
        final MessageSessionFactory messageSessionFactory = new TubeSingleSessionFactory(clientConfig);
        final MessageProducer messageProducer = messageSessionFactory.createProducer();
        final String topic = "test_topic";

        for (int i = 0; i < 10000; i++) {
            final String body = "This is a test message from single-session-factory! " + i;
            byte[] bodyData = StringUtils.getBytesUtf8(body);
            // 1. Producer在系统中一共4对指令，到master是要做注册，心跳，退出操作；
            messageProducer.publish(topic);  // 设定要发表的主题。可能会初始化信息，例如主题的分区等等信息。
            Message message = new Message(topic, bodyData);
            // 到broker只有发送消息
            MessageSentResult result = messageProducer.sendMessage(message);
//            messageProducer.sendMessage(message, );
            if (result.isSuccess()) {
                System.out.println("sync send message : " + message);
            }
            Thread.sleep(500);
        }
        messageProducer.shutdown();
    }
}
