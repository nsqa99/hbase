package com.viettel.model

import com.viettel.utils.HBaseCommonUtils
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ArrayBuffer

/**
 * @author anhnsq@viettel.com.vn
 */
class Order private (var orderId: Long, var username: String, var total: Double, var orderItems: ArrayBuffer[OrderItem], var created_time: Long) {
  def this(orderId: Long, userId: String, total: Double, created_time: Long) {
    this(orderId, userId, total, ArrayBuffer.empty[OrderItem], created_time)
  }
}

object Order extends HBaseCommonUtils[Order] {
  val TABLE_NAME: Array[Byte] = Bytes.toBytes("users")
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

    new Order(Bytes.toLong(orderId), keys(0), Bytes.toDouble(total), Bytes.toLong(createdTime))
  }

  override def hbaseRowKey(order: Order): Array[Byte] = {
    Bytes.toBytes(s"${order.username}+${order.created_time}+${order.orderId}")
  }
}