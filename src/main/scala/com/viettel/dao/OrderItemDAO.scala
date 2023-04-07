package com.viettel.dao

import com.viettel.model.Item.TABLE_NAME
import com.viettel.model.OrderItem.{AMOUNT_COL, INFO_FAM, PRICE_COL}
import com.viettel.model.{Item, OrderItem}
import com.viettel.utils.Utils
import com.viettel.utils.Utils.getHbaseTbl
import org.apache.hadoop.hbase.client.{Connection, Delete, Get, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.LogManager

class OrderItemDAO(connection: Connection) extends BaseDAO[OrderItem] {
  private val log = LogManager.getLogger(classOf[ItemDAO])

  override def getOne(orderItem: OrderItem): OrderItem = {
    val ordItemTbl = getHbaseTbl(connection, TABLE_NAME)

    val g = new Get(OrderItem.hbaseRowKey(orderItem))
    g.addFamily(INFO_FAM)
    val rs = ordItemTbl.get(g)
    if (rs.isEmpty) {
      log.warn(s"Item ${orderItem.itemId} of order ${orderItem.orderId} not found")
    }

    val extractedOrderItem = OrderItem.of(rs)
    ordItemTbl.close()

    extractedOrderItem
  }

  override def getAll(orderItem: OrderItem): List[OrderItem] = {
    Utils.getAll(connection = connection, tableName = TABLE_NAME, columnFamily = INFO_FAM, constructor = OrderItem.of)
  }

  override def addOne(orderItem: OrderItem): Unit = {
    val ordItemTbl = getHbaseTbl(connection, TABLE_NAME)

    val p = new Put(OrderItem.hbaseRowKey(orderItem))
    p.addColumn(INFO_FAM, AMOUNT_COL, Bytes.toBytes(orderItem.amount))
    p.addColumn(INFO_FAM, PRICE_COL, Bytes.toBytes(orderItem.price))
    ordItemTbl.put(p)
    log.debug(s"Added orderItem: item ${orderItem.itemId} of order ${orderItem.orderId}")

    ordItemTbl.close()
  }

  override def deleteOne(orderItem: OrderItem): Unit = {
    val ordItemTbl = getHbaseTbl(connection, TABLE_NAME)

    val d = new Delete(OrderItem.hbaseRowKey(orderItem))
    ordItemTbl.delete(d)
    log.debug(s"Deleted orderItem: item ${orderItem.itemId} of order ${orderItem.orderId}")

    ordItemTbl.close()
  }
}
