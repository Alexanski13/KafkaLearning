package com.alexanski.ws.emailnotification;

import com.alexanski.ws.core.ProductCreatedEvent;
import com.alexanski.ws.emailnotification.entity.ProcessedEventEntity;
import com.alexanski.ws.emailnotification.handler.ProductCreatedEventHandler;
import com.alexanski.ws.emailnotification.repository.ProcessedEventRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.*;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@EmbeddedKafka
@SpringBootTest(properties = "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}")
public class ProductsCreatedEventHandlerIntegrationTest {

    @MockitoBean
    private ProcessedEventRepository processedEventRepository;

    @MockitoBean
    private RestTemplate restTemplate;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @MockitoSpyBean
    private ProductCreatedEventHandler productCreatedEventHandler;

    @Test
    void testProductCreatedEventHandler_OnProductCreated_HandlesEvent() throws ExecutionException, InterruptedException {
        //Arrange
        ProductCreatedEvent productCreatedEvent = new ProductCreatedEvent();
        productCreatedEvent.setPrice(new BigDecimal(666));
        productCreatedEvent.setProductId(UUID.randomUUID().toString());
        productCreatedEvent.setQuantity(20);
        productCreatedEvent.setTitle("Chairs");

        String messageId = UUID.randomUUID().toString();
        String messageKey = productCreatedEvent.getProductId();

        ProducerRecord<String, Object> record = new ProducerRecord<>(
                "product-created-events-topic",
                messageKey,
                productCreatedEvent
        );

        record.headers().add("messageId", messageId.getBytes());
        record.headers().add(KafkaHeaders.RECEIVED_KEY, messageId.getBytes());

        ProcessedEventEntity processedEventEntity = new ProcessedEventEntity();
        Mockito.when(processedEventRepository.findByMessageId(anyString())).thenReturn(processedEventEntity);
        Mockito.when(processedEventRepository.save(any(ProcessedEventEntity.class))).thenReturn(null);

        String responseBody = "{\"key\":\"value\"}";
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<String> response = new ResponseEntity<>(responseBody, headers, HttpStatus.OK);
        Mockito.when(restTemplate.exchange(any(String.class), any(HttpMethod.class), isNull(), eq(String.class)))
                .thenReturn(response);
        // Act
        kafkaTemplate.send(record).get();

        // Assert
        ArgumentCaptor<String> messageIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> messageKeyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ProductCreatedEvent> eventCaptor = ArgumentCaptor.forClass(ProductCreatedEvent.class);

        verify(productCreatedEventHandler, timeout(5000).times(1)).handle(eventCaptor.capture(),
                messageIdCaptor.capture(),
                messageKeyCaptor.capture());

        assertEquals(messageId, messageIdCaptor.getValue());
        assertEquals(messageKey, messageKeyCaptor.getValue());
        assertEquals(productCreatedEvent.getProductId(), eventCaptor.getValue().getProductId());
        assertEquals(productCreatedEvent.getTitle(), eventCaptor.getValue().getTitle());
        assertEquals(productCreatedEvent.getPrice(), eventCaptor.getValue().getPrice());
        assertEquals(productCreatedEvent.getQuantity(), eventCaptor.getValue().getQuantity());
    }
}
