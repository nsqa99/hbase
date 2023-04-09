package com.viettel.model

import com.viettel.utils.HBaseCommonUtils
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes

/**
 * @author anhnsq@viettel.com.vn
 */
class OrderItem private (var orderId: Long, var amount: Int, var item: Item) {
  override def toString: String = {
    s"Order Item: {orderId=$orderId, item=$item, amount=$amount}"
  }
}

object OrderItem extends HBaseCommonUtils[OrderItem] {
  val TABLE_NAME: Array[Byte] = Bytes.toBytes("order_items")
  val ITEM_INFO: Array[Byte] = Bytes.toBytes("item_info")
  val AMOUNT_COL: Array[Byte] = Bytes.toBytes("amount")
  val PRICE_COL: Array[Byte] = Bytes.toBytes("price")
  val NAME_COL: Array[Byte] = Bytes.toBytes("name")

  override def hbaseRowKey(orderItem: OrderItem): Array[Byte] = {
    Bytes.toBytes(s"${orderItem.orderId}+${orderItem.item.itemId}")
  }

  def of(rs: Result): OrderItem = {
    val rowKey = Bytes.toString(rs.getRow)
    val keys = rowKey.split("\\+")
    val amount = rs.getValue(ITEM_INFO, AMOUNT_COL)
    val price = rs.getValue(ITEM_INFO, PRICE_COL)
    val name = rs.getValue(ITEM_INFO, NAME_COL)
    val item = Item.of(keys(1).toLong, Bytes.toString(name), Bytes.toDouble(price))

    new OrderItem(orderId = keys(0).toLong, amount = Bytes.toInt(amount), item)
  }

  def of(lineData: Array[String]): OrderItem = {
    new OrderItem(orderId = lineData(0).toLong, amount = lineData(1).toInt, Item.of(lineData(2).toLong))
  }
}
