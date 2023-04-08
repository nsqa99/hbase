package com.viettel.model

import com.viettel.utils.HBaseCommonUtils
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes

/**
 * @author anhnsq@viettel.com.vn
 */
class Item private (var itemId: Long, var name: String, var price: Double) {

}

object Item extends HBaseCommonUtils[Item] {
  val TABLE_NAME: Array[Byte] = Bytes.toBytes("items")
  val ITEM_INFO_FAM: Array[Byte] = Bytes.toBytes("info")
  val ITEM_ID_COL: Array[Byte] = Bytes.toBytes("id")
  val ITEM_NAME_COL: Array[Byte] = Bytes.toBytes("name")
  val ITEM_PRICE_COL: Array[Byte] = Bytes.toBytes("price")

  def of(rs: Result): Item = {
    val itemId = rs.getValue(ITEM_INFO_FAM, ITEM_ID_COL)
    val itemName = rs.getValue(ITEM_INFO_FAM, ITEM_NAME_COL)
    val itemPrice = rs.getValue(ITEM_INFO_FAM, ITEM_PRICE_COL)

    new Item(Bytes.toLong(itemId), Bytes.toString(itemName), Bytes.toDouble(itemPrice))
  }

  def of(itemId: Long, name: String, price: Double): Item = {
    new Item(itemId, name, price)
  }

  override def hbaseRowKey(item: Item): Array[Byte] = {
    Bytes.toBytes(item.itemId)
  }
}