package com.viettel.service
import com.viettel.dao.{OrderDAO, OrderItemDAO}
import com.viettel.model.Order

class OrderServiceImpl(orderDAO: OrderDAO, orderItemDAO: OrderItemDAO) extends OrderService {
  override def getUserOrdersWithinTimeRange(username: String, startTime: Long, endTime: Long): List[Order] = {
    val orders = orderDAO.getUserOrdersWithinTimeRange(username, startTime, endTime)
    val orderIds = orders.map(_.orderId)
    val orderItemsMap = orderItemDAO.getOrderItems(orderIds)
    orders.foreach(order => {
      val listOrderItemsOpt = orderItemsMap.get(order.orderId)
      if (listOrderItemsOpt.isDefined) {
        order.orderItems = listOrderItemsOpt.get
      }
    })

    orders
  }
}
