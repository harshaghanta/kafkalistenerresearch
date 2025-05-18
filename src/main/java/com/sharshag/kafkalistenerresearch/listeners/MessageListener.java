package com.sharshag.kafkalistenerresearch.listeners;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.sharshag.kafkalistenerresearch.MessageRecorder;
import com.sharshag.kafkalistenerresearch.models.TestMessage;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
@RequiredArgsConstructor
public class MessageListener {

    private final MessageRecorder messageRecorder;

    // @KafkaListener(topics = "TestTopic", groupId = "test-group", id = "testListener")
    public void listen(TestMessage message) throws InterruptedException {
        log.info( "Received message:" + message);
        Thread.sleep(15000); // Wait for 15 seconds
        messageRecorder.recordMessage(message);

    }
}