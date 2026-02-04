package com.alexanski.orders.saga;

import com.alexanski.core.dto.commands.*;
import com.alexanski.core.dto.events.*;
import com.alexanski.core.types.OrderStatus;
import com.alexanski.orders.service.OrderHistoryService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@KafkaListener(topics = {
        "${orders.events.topic.name}",
        "${products.events.topic.name}",
        "${payments.events.topic.name}"
})
public class OrderSaga {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    private final String productsCommandsTopicName;

    private final String paymentsCommandsTopicName;

    private final String orderCommandsTopicName;

    private final OrderHistoryService orderHistoryService;

    public OrderSaga(KafkaTemplate<String, Object> kafkaTemplate,
                     @Value("${products.commands.topic.name}") String productsCommandsTopicName,
                     @Value("${payments.commands.topic.name}") String paymentsCommandsTopicName,
                     @Value("${orders.commands.topic.name}") String orderCommandsTopicName,
                     OrderHistoryService orderHistoryService) {
        this.kafkaTemplate = kafkaTemplate;
        this.productsCommandsTopicName = productsCommandsTopicName;
        this.paymentsCommandsTopicName = paymentsCommandsTopicName;
        this.orderCommandsTopicName = orderCommandsTopicName;
        this.orderHistoryService = orderHistoryService;
    }

    @KafkaHandler
    public void handleEvent(@Payload OrderCreatedEvent event) {
        ReserveProductCommand reserveProductCommand = new ReserveProductCommand();
        reserveProductCommand.setProductId(event.getProductId());
        reserveProductCommand.setOrderId(event.getOrderId());
        reserveProductCommand.setProductQuantity(event.getProductQuantity());

        kafkaTemplate.send(productsCommandsTopicName, reserveProductCommand);
        orderHistoryService.add(event.getOrderId(), OrderStatus.CREATED);
    }

    @KafkaHandler
    public void handleEvent(@Payload ProductReservedEvent event) {
        ProcessPaymentCommand reserveProductCommand = new ProcessPaymentCommand(
                event.getOrderId(),
                event.getProductId(),
                event.getProductPrice(),
                event.getProductQuantity()
        );
        kafkaTemplate.send(paymentsCommandsTopicName, reserveProductCommand);
    }

    @KafkaHandler
    public void handleEvent(@Payload PaymentProcessedEvent event) {
        ApproveOrderCommand approveOrderCommand = new ApproveOrderCommand(
                event.getOrderId()
        );
        kafkaTemplate.send(orderCommandsTopicName, approveOrderCommand);
    }

    @KafkaHandler
    public void handleEvent(@Payload OrderApprovedEvent event) {
        orderHistoryService.add(event.getOrderId(), OrderStatus.APPROVED);
    }

    @KafkaHandler
    public void handleEvent(@Payload PaymentFailedEvent event) {
        CancelProductReservationCommand command = new CancelProductReservationCommand(
                event.getProductId(),
                event.getOrderId(),
                event.getProductQuantity()
        );
        kafkaTemplate.send(productsCommandsTopicName, command);
    }

    @KafkaHandler
    public void handleEvent(@Payload ProductReservationCancelledEvent event) {
        RejectOrderCommand command = new RejectOrderCommand(event.getOrderId());
        kafkaTemplate.send(orderCommandsTopicName, command);
        orderHistoryService.add(event.getOrderId(), OrderStatus.REJECTED);
    }
}
