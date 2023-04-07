package com.viettel.dao

import com.viettel.model.Order
import com.viettel.model.Order.{INFO_FAM, ORDER_CREATED_TIME_COL, ORDER_ID_COL, ORDER_TOTAL_COL, TABLE_NAME}
import com.viettel.utils.Utils
import com.viettel.utils.Utils.getHbaseTbl
import org.apache.hadoop.hbase.client.{Connection, Delete, Get, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.LogManager

class OrderDAO(connection: Connection) extends AbstractDAO[Order] {
  private val log = LogManager.getLogger(classOf[OrderDAO])

  override def getOne(order: Order): Order = {
    val orderTbl = getHbaseTbl(connection, TABLE_NAME)
    val g = new Get(Order.hbaseRowKey(order))
    val rs = orderTbl.get(g)
    val extractedOrder = Order.of(rs)
    orderTbl.close()

    extractedOrder
  }

  override def getAll(order: Order): List[Order] = {
    Utils.getAll[Order](connection = connection, tableName = TABLE_NAME, columnFamily = INFO_FAM)
  }

  override def addOne(order: Order): Unit = {
    // require create column family before execute this
    val userTbl = getHbaseTbl(connection, TABLE_NAME)

    val p = new Put(Order.hbaseRowKey(order))
    p.addColumn(INFO_FAM, ORDER_ID_COL, Bytes.toBytes(order.orderId))
    p.addColumn(INFO_FAM, ORDER_TOTAL_COL, Bytes.toBytes(order.total))
    p.addColumn(INFO_FAM, ORDER_CREATED_TIME_COL, Bytes.toBytes(order.created_time))
    order.orderItems.foreach(orderItem => {
      p.addColumn(Bytes.toBytes(orderItem.itemId), Bytes.toBytes(orderItem.amount), Bytes.toBytes(orderItem.price * orderItem.amount))
    })
    userTbl.put(p)
    log.debug(s"Added order ${order.orderId} of user ${order.username}")

    userTbl.close()
  }

  override def deleteOne(order: Order): Unit = {
    val userTbl = getHbaseTbl(connection, TABLE_NAME)
    val d = new Delete(Order.hbaseRowKey(order))
    userTbl.delete(d)
    log.debug(s"Deleted order ${order.orderId} of user ${order.username}")

    userTbl.close()
  }
}
