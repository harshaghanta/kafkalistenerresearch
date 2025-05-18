package com.sharshag.kafkalistenerresearch;

import java.util.HashMap;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.sharshag.kafkalistenerresearch.models.TestMessage;

import lombok.Getter;

@Component
@Getter
public class MessageRecorder {

    Map<Integer, Integer> messageCountMap = new HashMap<>();


    public void recordMessage(TestMessage message) {
        int messageId = message.getMessageId();
        // Simulate recording the message (e.g., saving to a database or file)
        messageCountMap.put(messageId, messageCountMap.getOrDefault(messageId, 0) + 1);
    }


}
