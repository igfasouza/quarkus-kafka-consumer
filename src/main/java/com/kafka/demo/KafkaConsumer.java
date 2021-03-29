package com.kafka.demo;

import java.util.Optional;
import javax.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import io.smallrye.reactive.messaging.annotations.Broadcast;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecordMetadata;

@ApplicationScoped
public class KafkaConsumer {

    @Incoming("mytopic-subscriber")
    @Outgoing("internal-test")
    @Broadcast
    public String process(Message<String> incoming) {
    	String key = getKey(incoming);
    	return key + ";" + incoming.getPayload();
    }
    
    @SuppressWarnings("rawtypes")
    private String getKey(Message<String> incoming) {
		Optional<IncomingKafkaRecordMetadata> metadata = incoming.getMetadata(IncomingKafkaRecordMetadata.class);
    	IncomingKafkaRecordMetadata<?, ?> record = metadata.orElseThrow(RuntimeException::new);
    	return record.getKey().toString();
    }
	
}