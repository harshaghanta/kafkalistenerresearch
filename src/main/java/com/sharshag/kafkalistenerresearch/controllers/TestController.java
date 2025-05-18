package com.sharshag.kafkalistenerresearch.controllers;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.sharshag.kafkalistenerresearch.MessageRecorder;
import com.sharshag.kafkalistenerresearch.SamplePayload;
import com.sharshag.kafkalistenerresearch.components.DynamicKafkaListenerService;
import com.sharshag.kafkalistenerresearch.components.KafkaListenerControl;
import com.sharshag.kafkalistenerresearch.models.TestMessage;


@RestController
public class TestController {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final MessageRecorder messageRecorder;
    private KafkaListenerControl kafkaListenerControl;
    private final DynamicKafkaListenerService dynamicKafkaListenerService;

    public TestController(KafkaTemplate<String, Object> kafkaTemplate, KafkaListenerControl kafkaListenerControl,
    DynamicKafkaListenerService dynamicKafkaListenerService) {
        this.kafkaTemplate = kafkaTemplate;
        this.messageRecorder = new MessageRecorder();
        this.kafkaListenerControl = kafkaListenerControl;
        this.dynamicKafkaListenerService = dynamicKafkaListenerService;
    }

    @PostMapping("/send/{topicName}")
    public ResponseEntity<String> postMessage(@PathVariable String topicName, @RequestBody SamplePayload payload) {
        RecordMetadata recordMetadata = null;
        String message = null;
        
        CompletableFuture<SendResult<String, Object>> future = null;
        //Send a simple message to the topic.
        // future = kafkaTemplate.send(topicName, payload);

        //Send a message to the topic with a key and payload
        ProducerRecord<String, Object> record = new ProducerRecord<>(topicName, payload.key(),  payload);        
        future = kafkaTemplate.send(record);

        
        try {
            recordMetadata = future.get().getRecordMetadata();
            message = "Message sent to the topic: " + recordMetadata.topic() + "in partition: " + recordMetadata.partition() +  " with the offset " + recordMetadata.offset();

        } catch (InterruptedException | ExecutionException e) {
            
            message = "Error sending the message";
        }

        return ResponseEntity.ok().body(message);
        
    }



    @PostMapping("/sendBulkMessages/{messageCount}")
    public String sendBulkMessages(@PathVariable int messageCount) {
        for (int i = 0; i < messageCount; i++) {

            TestMessage testMessage = new TestMessage((i + 1));
            kafkaTemplate.send("TestTopic", testMessage);
        }
        return "Messages sent successfully!";
    }

    @GetMapping("/printMessages")
    public String printMessages() {
        return messageRecorder.getMessageCountMap().toString();
    }

    @PostMapping("/listenerRemoteControl/{listenerId}/{action}")
    public String listenerRemoteControl(@PathVariable String listenerId,
            @PathVariable(name = "action", required = false) String action) {

        if (action.equals("start")) {
            kafkaListenerControl.startListener(listenerId);
        } else if (action.equals("stop")) {
            kafkaListenerControl.stopListener(listenerId);
        } else {
            return "Invalid action. Use 'start' or 'stop'.";
        }

        return "success";
    }

    @PostMapping("/registerListener/{topicName}")
    public ResponseEntity<String> registerListener(@PathVariable String topicName) {
        try {
            dynamicKafkaListenerService.registerListener(topicName, "mycustomgroup");
        } catch (NoSuchMethodException e) {
            return ResponseEntity.status(500).body("Error registering listener: " + e.getMessage());
        }
        return ResponseEntity.ok().body("Listener registered successfully");
    }

}