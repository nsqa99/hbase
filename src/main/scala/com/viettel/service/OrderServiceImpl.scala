package com.viettel.service
import com.viettel.dao.{OrderDAO, OrderItemDAO}
import com.viettel.model.{Order, OrderItem}
import org.apache.hadoop.hbase.TableName

class OrderServiceImpl(orderDAO: OrderDAO, orderItemDAO: OrderItemDAO, admin: HBaseAdminService) extends OrderService {
  override def getUserOrdersWithinTimeRange(username: String, startTime: Long, endTime: Long): List[Order] = {
    val orders = orderDAO.getUserOrdersWithinTimeRange(username, startTime, endTime)
    val orderIds = orders.map(_.orderId)
    val orderItemsMap = orderItemDAO.getOrderItems(orderIds)
    orders.foreach(order => {
      val listOrderItemsOpt = orderItemsMap.get(order.orderId)
      if (listOrderItemsOpt.isDefined) {
        order.orderItems ++= listOrderItemsOpt.get
      }
    })

    orders
  }

  override def insertOrders(orders: List[Order]): Unit = {
    orders.foreach(orderDAO.addOne)
  }

  override def insertOrderItems(orderItems: List[OrderItem]): Unit = {
    orderItems.foreach(orderItemDAO.addOne)
  }

  override def createOrderTable(): Unit = {
    admin.createTable(TableName.valueOf(Order.TABLE_NAME), Order.INFO_FAM)
  }

  override def createOrderItemTable(): Unit = {
    admin.createTable(TableName.valueOf(OrderItem.TABLE_NAME), OrderItem.ITEM_INFO)
  }
}
