package com.viettel.model

import com.viettel.utils.HBaseCommonUtils
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ArrayBuffer

/**
 * @author anhnsq@viettel.com.vn
 */
class Order private(var orderId: Long, var username: String, var total: Double,
                    var orderItems: ArrayBuffer[OrderItem], var createdTime: Long) {
  def this(orderId: Long, username: String, total: Double, createdTime: Long) {
    this(orderId, username, total, ArrayBuffer.empty[OrderItem], createdTime)
  }

  override def toString: String = {
    s"Order: {created_time=$createdTime, orderId=$orderId, user=$username, total=$total, orderItems=${orderItems}"
  }
}

object Order extends HBaseCommonUtils[Order] {
  val TABLE_NAME: Array[Byte] = Bytes.toBytes("orders")
  val INFO_FAM: Array[Byte] = Bytes.toBytes("info")
  val ORDER_ID_COL: Array[Byte] = Bytes.toBytes("order_id")
  val USER_ID_COL: Array[Byte] = Bytes.toBytes("order_id")
  val ORDER_TOTAL_COL: Array[Byte] = Bytes.toBytes("total")
  val ORDER_CREATED_TIME_COL: Array[Byte] = Bytes.toBytes("created_time")

  def of(rs: Result): Order = {
    val rowKey = Bytes.toString(rs.getRow)
    val keys = rowKey.split("\\+")
    val orderId = rs.getValue(INFO_FAM, ORDER_ID_COL)
    val total = rs.getValue(INFO_FAM, ORDER_TOTAL_COL)
    val createdTime = rs.getValue(INFO_FAM, ORDER_CREATED_TIME_COL)

    new Order(orderId = Bytes.toLong(orderId), username = keys(0), total = Bytes.toDouble(total),
      createdTime = Bytes.toLong(createdTime))
  }

  def of(lineData: Array[String]): Order = {
    new Order(orderId = lineData(0).toLong, username = lineData(1), total = lineData(2).toDouble,
      createdTime = lineData(3).toLong)
  }

  override def hbaseRowKey(order: Order): Array[Byte] = {
    Bytes.toBytes(s"${order.username}+${order.createdTime}+${order.orderId}")
  }
}