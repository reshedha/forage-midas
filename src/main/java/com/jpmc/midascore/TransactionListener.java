package com.jpmc.midascore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.midascore.foundation.Transaction; // adjust import if Transaction is in a different package
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class TransactionListener {

    private final ObjectMapper objectMapper = new ObjectMapper();

    // thread-safe list to inspect received transactions (use debugger or getter)
    private static final List<Transaction> RECEIVED = new CopyOnWriteArrayList<>();

    @Value("${general.kafka-topic}")
    private String topic; // injected from application.yml or test properties

    // Listen to the topic configured in application.yml
    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    public void onMessage(String payload) {
        try {
            Transaction tx = objectMapper.readValue(payload, Transaction.class);
            // store it for debugging/inspection
            RECEIVED.add(tx);
            // simple logging so it also appears in test logs
            System.out.println("TransactionListener received: " + tx);
        } catch (Exception e) {
            System.err.println("Failed to deserialize incoming message: " + e.getMessage());
            // you could throw RuntimeException here if you want the test to fail on bad payloads
        }
    }

    // helper for tests / debugger
    public static List<Transaction> getReceived() {
        return RECEIVED;
    }
}
