package com.viettel.dao

import com.viettel.model.Order
import com.viettel.model.Order.{INFO_FAM, ORDER_CREATED_TIME_COL, ORDER_ID_COL, ORDER_TOTAL_COL, TABLE_NAME}
import com.viettel.utils.Utils
import com.viettel.utils.Utils.getHbaseTbl
import org.apache.hadoop.hbase.client.{Connection, Delete, Get, Put, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.LogManager

class OrderDAO(connection: Connection) extends BaseDAO[Order] {
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
    Utils.getAll[Order](connection = connection, tableName = TABLE_NAME, columnFamily = INFO_FAM, constructor = Order.of)
  }

  override def addOne(order: Order): Unit = {
    // require create column family before execute this
    val orderTbl = getHbaseTbl(connection, TABLE_NAME)

    val p = new Put(Order.hbaseRowKey(order))
    p.addColumn(INFO_FAM, ORDER_ID_COL, Bytes.toBytes(order.orderId))
    p.addColumn(INFO_FAM, ORDER_TOTAL_COL, Bytes.toBytes(order.total))
    p.addColumn(INFO_FAM, ORDER_CREATED_TIME_COL, Bytes.toBytes(order.createdTime))
    order.orderItems.foreach(orderItem => {
      p.addColumn(Bytes.toBytes(s"${orderItem.itemId}_item"), Bytes.toBytes(orderItem.amount),
        Bytes.toBytes(orderItem.price * orderItem.amount))
    })
    orderTbl.put(p)
    log.debug(s"Added order ${order.orderId} of user ${order.username}")

    orderTbl.close()
  }

  override def deleteOne(order: Order): Unit = {
    val orderTbl = getHbaseTbl(connection, TABLE_NAME)
    val d = new Delete(Order.hbaseRowKey(order))
    orderTbl.delete(d)
    log.debug(s"Deleted order ${order.orderId} of user ${order.username}")

    orderTbl.close()
  }

  def getUserOrdersWithinTimeRange(username: String, startTime: Long, endTime: Long): List[Order] = {
    val orderTbl = getHbaseTbl(connection, TABLE_NAME)

    val startRow = Bytes.toBytes(s"$username+$startTime")
    val stopRow = Bytes.toBytes(s"$username+${endTime}")
    val sc = new Scan()
      .withStartRow(startRow)
      .withStopRow(stopRow, true)
    sc.setRowPrefixFilter(Bytes.toBytes(username))

    val orderResults = orderTbl.getScanner(sc)
    val listBuilder = List.newBuilder[Order]
    orderResults.forEach(rs => {
      listBuilder += Order.of(rs)
    })
    val rs = listBuilder.result()
    // get order items by order ids

    orderTbl.close()
    rs
  }
}
