package org.apache.inlong.tubemq.example;

import org.apache.commons.codec.binary.StringUtils;
import org.apache.inlong.tubemq.client.common.PeerInfo;
import org.apache.inlong.tubemq.client.config.ConsumerConfig;
import org.apache.inlong.tubemq.client.config.TubeClientConfig;
import org.apache.inlong.tubemq.client.consumer.ConsumePosition;
import org.apache.inlong.tubemq.client.consumer.MessageListener;
import org.apache.inlong.tubemq.client.consumer.PushMessageConsumer;
import org.apache.inlong.tubemq.client.factory.MessageSessionFactory;
import org.apache.inlong.tubemq.client.factory.TubeSingleSessionFactory;
import org.apache.inlong.tubemq.client.producer.MessageProducer;
import org.apache.inlong.tubemq.client.producer.MessageSentResult;
import org.apache.inlong.tubemq.corebase.Message;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public class TestPushConsumerExample {
    public static void main(String[] args) throws Throwable {
        test(null);
    }


    public static void test(String[] args) throws Throwable {
        final String masterHostAndPort = "localhost:8715";
        final String topic = "test_topic";
        final String group = "test_group";
        final ConsumerConfig consumerConfig = new ConsumerConfig(masterHostAndPort, group);
        consumerConfig.setConsumePosition(ConsumePosition.CONSUMER_FROM_LATEST_OFFSET);
        final MessageSessionFactory messageSessionFactory = new TubeSingleSessionFactory(consumerConfig);
        final PushMessageConsumer pushConsumer = messageSessionFactory.createPushConsumer(consumerConfig);
        pushConsumer.subscribe(topic, null, new MessageListener() {
            @Override
            public void receiveMessages(PeerInfo peerInfo, List<Message> messages) throws InterruptedException {
                for (Message message : messages) {
                    System.out.println("received message : " + new String(message.getData()));
                }
            }

            @Override
            public Executor getExecutor() {
                return null;
            }

            @Override
            public void stop() {
                //
            }
        });
        pushConsumer.completeSubscribe();
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(10, TimeUnit.MINUTES);
    }
}
