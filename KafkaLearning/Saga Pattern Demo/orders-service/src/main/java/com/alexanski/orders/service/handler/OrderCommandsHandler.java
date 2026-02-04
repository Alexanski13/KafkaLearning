package com.alexanski.orders.service.handler;

import com.alexanski.core.dto.commands.ApproveOrderCommand;
import com.alexanski.core.dto.commands.RejectOrderCommand;
import com.alexanski.orders.service.OrderService;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@KafkaListener(topics = "${orders.commands.topic.name}")
public class OrderCommandsHandler {

    private final OrderService orderService;

    public OrderCommandsHandler(OrderService orderService) {
        this.orderService = orderService;
    }

    @KafkaHandler
    public void handleCommand(@Payload ApproveOrderCommand command) {
        orderService.approveOrder(command.getOrderId());
    }

    @KafkaHandler
    public void handleCommand(@Payload RejectOrderCommand command) {
        orderService.rejectOrder(command.getOrderId());
    }
}
