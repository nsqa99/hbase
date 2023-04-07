package com.viettel.model

import com.viettel.utils.HBaseCommonUtils
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes

/**
 * @author anhnsq@viettel.com.vn
 */
class OrderItem private (var orderId: Long, var itemId: Long, var amount: Int, var price: Double)

object OrderItem extends HBaseCommonUtils[OrderItem] {
  val TABLE_NAME: Array[Byte] = Bytes.toBytes("order_items")
  val INFO_FAM: Array[Byte] = Bytes.toBytes("info")
  val AMOUNT_COL: Array[Byte] = Bytes.toBytes("amount")
  val PRICE_COL: Array[Byte] = Bytes.toBytes("price")

  override def hbaseRowKey(orderItem: OrderItem): Array[Byte] = {
    Bytes.toBytes(s"${orderItem.orderId}+${orderItem.itemId}")
  }

  def of(rs: Result): OrderItem = {
    val rowKey = Bytes.toString(rs.getRow)
    val keys = rowKey.split("\\+")
    val amount = rs.getValue(INFO_FAM, AMOUNT_COL)
    val price = rs.getValue(INFO_FAM, PRICE_COL)

    new OrderItem(orderId = keys(0).toLong, itemId = keys(1).toLong,
      amount = Bytes.toInt(amount), price = Bytes.toDouble(price))
  }
}
