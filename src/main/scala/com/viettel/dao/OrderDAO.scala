package com.viettel.dao

import com.viettel.model.Order
import com.viettel.model.Order._
import com.viettel.utils.Utils
import com.viettel.utils.Utils.getHbaseTbl
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.LoggerFactory

class OrderDAO(connection: Connection) extends BaseDAO[Order] {
  private val log = LoggerFactory.getLogger(classOf[OrderDAO])

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
    val orderTbl = getHbaseTbl(connection, TABLE_NAME)

    val p = new Put(Order.hbaseRowKey(order))
    p.addColumn(INFO_FAM, ORDER_ID_COL, Bytes.toBytes(order.orderId))
    p.addColumn(INFO_FAM, ORDER_TOTAL_COL, Bytes.toBytes(order.total))
    p.addColumn(INFO_FAM, ORDER_CREATED_TIME_COL, Bytes.toBytes(order.createdTime))

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
    val stopRow = Bytes.toBytes(s"$username+$endTime")
    val sc = new Scan()
      .withStartRow(startRow)
      .withStopRow(stopRow, true)

    val orderResults = orderTbl.getScanner(sc)
    val listBuilder = List.newBuilder[Order]
    orderResults.forEach(rs => {
      listBuilder += Order.of(rs)
    })
    val rs = listBuilder.result()

    orderTbl.close()
    rs
  }
}
