package com.alexanski.payments.service.handler;

import com.alexanski.core.dto.Payment;
import com.alexanski.core.dto.commands.ProcessPaymentCommand;
import com.alexanski.core.dto.events.PaymentFailedEvent;
import com.alexanski.core.dto.events.PaymentProcessedEvent;
import com.alexanski.core.exceptions.CreditCardProcessorUnavailableException;
import com.alexanski.payments.service.PaymentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@KafkaListener(topics = {"${payments.commands.topic.name}"})
public class PaymentsCommandsHandler {

    private final PaymentService paymentService;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String paymentsEventsTopicName;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public PaymentsCommandsHandler(PaymentService paymentService, KafkaTemplate<String, Object> kafkaTemplate,
                                   @Value("${payments.events.topic.name}") String paymentsEventsTopicName) {
        this.paymentService = paymentService;
        this.kafkaTemplate = kafkaTemplate;
        this.paymentsEventsTopicName = paymentsEventsTopicName;
    }

    @KafkaHandler
    public void handleCommand(@Payload ProcessPaymentCommand command) {
        try {
            Payment payment = new Payment(
                    command.getOrderId(),
                    command.getProductId(),
                    command.getProductPrice(),
                    command.getProductQuantity()
            );
            Payment processedPayment = paymentService.process(payment);
            PaymentProcessedEvent paymentProcessedEvent = new PaymentProcessedEvent(
                    processedPayment.getOrderId(),
                    processedPayment.getId()
            );
            kafkaTemplate.send(paymentsEventsTopicName, paymentProcessedEvent);
        } catch (CreditCardProcessorUnavailableException e) {
            PaymentFailedEvent paymentFailedEvent = new PaymentFailedEvent(
                    command.getOrderId(),
                    command.getProductId(),
                    command.getProductQuantity()
            );
            kafkaTemplate.send(paymentsEventsTopicName, paymentFailedEvent);
            logger.error(e.getLocalizedMessage(), e);
        }
    }
}
