package com.alexanski.orders.service;

import com.alexanski.core.dto.Order;
import com.alexanski.core.dto.events.OrderApprovedEvent;
import com.alexanski.core.dto.events.OrderCreatedEvent;
import com.alexanski.core.types.OrderStatus;
import com.alexanski.orders.dao.jpa.entity.OrderEntity;
import com.alexanski.orders.dao.jpa.repository.OrderRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.util.UUID;

@Service
public class OrderServiceImpl implements OrderService {
    private final OrderRepository orderRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String ordersEventsTopicName;

    public OrderServiceImpl(OrderRepository orderRepository, KafkaTemplate<String, Object> kafkaTemplate,
                            @Value("${orders.events.topic.name}") String ordersEventsTopicName) {
        this.orderRepository = orderRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.ordersEventsTopicName = ordersEventsTopicName;
    }

    @Override
    public Order placeOrder(Order order) {
        OrderEntity entity = new OrderEntity();
        entity.setCustomerId(order.getCustomerId());
        entity.setProductId(order.getProductId());
        entity.setProductQuantity(order.getProductQuantity());
        entity.setStatus(OrderStatus.CREATED);
        orderRepository.save(entity);

        OrderCreatedEvent placedOrder = new OrderCreatedEvent(
                entity.getId(),
                entity.getCustomerId(),
                order.getProductId(),
                order.getProductQuantity()
        );

        kafkaTemplate.send(ordersEventsTopicName, placedOrder);

        return new Order(
                entity.getId(),
                entity.getCustomerId(),
                entity.getProductId(),
                entity.getProductQuantity(),
                entity.getStatus());
    }

    @Override
    public void approveOrder(UUID orderId) {
        OrderEntity order = orderRepository.findById(orderId).orElse(null);
        Assert.notNull(order, "No order is found with id %s in the database table".formatted(orderId));
        order.setStatus(OrderStatus.APPROVED);
        orderRepository.save(order);
        OrderApprovedEvent orderApprovedEvent = new OrderApprovedEvent(orderId);
        kafkaTemplate.send(ordersEventsTopicName, orderApprovedEvent);
    }

    @Override
    public void rejectOrder(UUID orderId) {
        OrderEntity order = orderRepository.findById(orderId).orElse(null);
        Assert.notNull(order, "No order is found with id: %s".formatted(orderId));
        order.setStatus(OrderStatus.REJECTED);
        orderRepository.save(order);
    }

}
