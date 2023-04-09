package com.viettel.service

import com.viettel.model.{Order, OrderItem}

trait OrderService {
  def createOrderTable(): Unit
  def createOrderItemTable(): Unit
  def insertOrders(orders: List[Order]): Unit
  def insertOrderItems(orderItems: List[OrderItem]): Unit
  def getUserOrdersWithinTimeRange(username: String, startTime: Long, endTime: Long): List[Order]
}
