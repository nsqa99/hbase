package com.viettel.dao

import com.viettel.model.OrderItem
import com.viettel.model.OrderItem.{AMOUNT_COL, ITEM_INFO, NAME_COL, PRICE_COL, TABLE_NAME}
import com.viettel.utils.Utils
import com.viettel.utils.Utils.getHbaseTbl
import org.apache.hadoop.hbase.client.{Connection, Delete, Get, Put, Scan}
import org.apache.hadoop.hbase.filter.{FilterList, PrefixFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.LogManager

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class OrderItemDAO(connection: Connection) extends BaseDAO[OrderItem] {
  private val log = LogManager.getLogger(classOf[ItemDAO])

  override def getOne(orderItem: OrderItem): OrderItem = {
    val ordItemTbl = getHbaseTbl(connection, TABLE_NAME)

    val g = new Get(OrderItem.hbaseRowKey(orderItem))
    g.addFamily(ITEM_INFO)
    val rs = ordItemTbl.get(g)
    if (rs.isEmpty) {
      log.warn(s"Item ${orderItem.item.itemId} of order ${orderItem.orderId} not found")
    }

    val extractedOrderItem = OrderItem.of(rs)
    ordItemTbl.close()

    extractedOrderItem
  }

  override def getAll(orderItem: OrderItem): List[OrderItem] = {
    Utils.getAll(connection = connection, tableName = TABLE_NAME, columnFamily = ITEM_INFO, constructor = OrderItem.of)
  }

  override def addOne(orderItem: OrderItem): Unit = {
    val ordItemTbl = getHbaseTbl(connection, TABLE_NAME)

    val p = new Put(OrderItem.hbaseRowKey(orderItem))
    p.addColumn(ITEM_INFO, AMOUNT_COL, Bytes.toBytes(orderItem.amount))
    p.addColumn(ITEM_INFO, PRICE_COL, Bytes.toBytes(orderItem.item.price))
    p.addColumn(ITEM_INFO, NAME_COL, Bytes.toBytes(orderItem.item.name))
    ordItemTbl.put(p)
    log.debug(s"Added orderItem: item ${orderItem.item.itemId} of order ${orderItem.orderId}")

    ordItemTbl.close()
  }

  override def deleteOne(orderItem: OrderItem): Unit = {
    val ordItemTbl = getHbaseTbl(connection, TABLE_NAME)

    val d = new Delete(OrderItem.hbaseRowKey(orderItem))
    ordItemTbl.delete(d)
    log.debug(s"Deleted orderItem: item ${orderItem.item.itemId} of order ${orderItem.orderId}")

    ordItemTbl.close()
  }

  def getOrderItems(orderIds: List[Long]): mutable.Map[Long, ArrayBuffer[OrderItem]] = {
    val ordItemTbl = getHbaseTbl(connection, TABLE_NAME)
    val sc = new Scan
    val filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE)
    orderIds.foreach(orderId => {
      filterList.addFilter(new PrefixFilter(Bytes.toBytes(String.valueOf(orderId))))
    })
    sc.setFilter(filterList)
    val results = ordItemTbl.getScanner(sc)
    val builder = List.newBuilder[OrderItem]
    results.forEach(rs => {
      builder += OrderItem.of(rs)
    })
    val rs = builder.result()
    ordItemTbl.close()

    val orderItemMap = mutable.Map.empty[Long, ArrayBuffer[OrderItem]]
    rs.foreach(orderItem => updateMap(orderItemMap, orderItem.orderId, orderItem))

    orderItemMap
  }

  private def updateMap(map: mutable.Map[Long, ArrayBuffer[OrderItem]], key: Long, value: OrderItem): Unit = {
    if (!map.contains(key)) {
      map += (key -> ArrayBuffer(value))
    } else {
      map(key) += value
    }
  }
}
