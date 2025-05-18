package com.sharshag.kafkalistenerresearch.components;

import java.lang.reflect.Method;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.stereotype.Component;

import com.sharshag.kafkalistenerresearch.listeners.MyCustomListener;

@Component
public class DynamicKafkaListenerService {

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Autowired
    private ConcurrentKafkaListenerContainerFactory<String, Object> factory;

    @Autowired
    private MessageHandlerMethodFactory messageHandlerMethodFactory;

    @Autowired
    private MyCustomListener myCustomListener;

    public void registerListener(String topic, String groupId) throws NoSuchMethodException {
        MethodKafkaListenerEndpoint<String, Object> endpoint = new MethodKafkaListenerEndpoint<>();
        endpoint.setId(topic + "-" + groupId);
        endpoint.setGroupId(groupId);
        endpoint.setTopics(topic);
        endpoint.setBean(myCustomListener);
        Properties consumerProperties = new Properties();
        consumerProperties.put("max.poll.interval.ms", "300000"); // 5 minutes
        consumerProperties.put("max.poll.records", "10");

        endpoint.setConsumerProperties(consumerProperties);
        Method method = MyCustomListener.class.getMethod("handleMessage", ConsumerRecord.class);
        endpoint.setMethod(method);
        endpoint.setMessageHandlerMethodFactory(messageHandlerMethodFactory);

        registry.registerListenerContainer(endpoint, factory, true);
    }
}
