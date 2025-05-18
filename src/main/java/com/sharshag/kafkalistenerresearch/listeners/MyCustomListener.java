package com.sharshag.kafkalistenerresearch.listeners;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

import com.sharshag.kafkalistenerresearch.SamplePayload;
import com.sharshag.kafkalistenerresearch.models.TestMessage;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class MyCustomListener {

    public void handleMessage(ConsumerRecord<String, Object> consumerRecord) {
        // Handle the message here
        Object receivedObject = consumerRecord.value();
        try {
            Thread.sleep(15000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        if (receivedObject instanceof SamplePayload) {
            SamplePayload payload = (SamplePayload) receivedObject;
            log.info("Received SamplePayload with key: {} and value: {}", payload.key(), payload.content());
        }
        else if(receivedObject instanceof TestMessage) {
            TestMessage testMessage = (TestMessage) receivedObject;
            log.info("Received TestMessage with id: {}", testMessage.getMessageId());
        }
        else {
            log.info("Received object of type: {}", receivedObject.getClass().getName());
        }

        
    }
}
