package com.alexanski.orders.service;

import com.alexanski.core.types.OrderStatus;
import com.alexanski.orders.dto.OrderHistory;

import java.util.List;
import java.util.UUID;

public interface OrderHistoryService {
    void add(UUID orderId, OrderStatus orderStatus);

    List<OrderHistory> findByOrderId(UUID orderId);
}
