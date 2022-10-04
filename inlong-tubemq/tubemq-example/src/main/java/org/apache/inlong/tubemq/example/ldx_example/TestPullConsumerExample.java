package org.apache.inlong.tubemq.example;

import org.apache.inlong.tubemq.client.config.ConsumerConfig;
import org.apache.inlong.tubemq.client.consumer.ConsumePosition;
import org.apache.inlong.tubemq.client.consumer.ConsumerResult;
import org.apache.inlong.tubemq.client.consumer.PullMessageConsumer;
import org.apache.inlong.tubemq.client.factory.MessageSessionFactory;
import org.apache.inlong.tubemq.client.factory.TubeSingleSessionFactory;
import org.apache.inlong.tubemq.corebase.Message;
import org.apache.inlong.tubemq.corebase.utils.ThreadUtils;

import java.util.Arrays;
import java.util.List;

public class TestPullConsumerExample {
    public static void main(String[] args) throws Throwable {
        final String masterHostAndPort = "localhost:8715";
        final String topic = "test_topic";
        final String group = "test_group";
        final ConsumerConfig consumerConfig = new ConsumerConfig(masterHostAndPort, group);
        consumerConfig.setConsumePosition(ConsumePosition.CONSUMER_FROM_LATEST_OFFSET);
//        consumerConfig.setConsumePosition(ConsumePosition.CONSUMER_FROM_FIRST_OFFSET);
        final MessageSessionFactory messageSessionFactory = new TubeSingleSessionFactory(consumerConfig);
        final PullMessageConsumer messagePullConsumer = messageSessionFactory.createPullConsumer(consumerConfig);
        messagePullConsumer.subscribe(topic, null);
        messagePullConsumer.completeSubscribe();
        // wait for client to join the exact consumer queue that consumer group allocated
        while (!messagePullConsumer.isPartitionsReady(1000)) {
            ThreadUtils.sleep(1000);
        }
        while (true) {
            ConsumerResult result = messagePullConsumer.getMessage();
            if (result.isSuccess()) {
                List<Message> messageList = result.getMessageList();
                for (Message message : messageList) {
                    System.out.println("received message : " + new String(message.getData()));
                }
                messagePullConsumer.confirmConsume(result.getConfirmContext(), true);
            }
        }
    }
}
