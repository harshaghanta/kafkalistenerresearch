package com.sharshag.kafkalistenerresearch.components;

import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class KafkaListenerControl {

    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    KafkaListenerControl(KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry) {
        this.kafkaListenerEndpointRegistry = kafkaListenerEndpointRegistry;
    }

    public void startListener(String listenerId) {

        MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer(listenerId);
        if (listenerContainer == null) {
            log.warn("Listener with id {} not found.", listenerId);
            return;
        }

        if (listenerContainer.isRunning()) {
            log.info("Listner {} already running.", listenerId);
            return;
        }

        listenerContainer.start();
        log.info("Listener {} started.", listenerId);

    }

    public void stopListener(String listenerId) {

        MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer(listenerId);
        if (listenerContainer == null) {
            log.warn("Listener with id {} not found.", listenerId);
            return;
        }

        if (!listenerContainer.isRunning()) {
            log.info("Listner {} already stopped.", listenerId);
            return;
        }

        listenerContainer.stop();
        log.info("Listener {} stopped.", listenerId);

    }

}
