package com.viettel.dao

import com.viettel.model.Item
import com.viettel.model.Item._
import com.viettel.utils.Utils._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.LogManager

/**
 * @author anhnsq@viettel.com.vn
 */
class ItemDAO(connection: Connection) extends AbstractDAO[Item] {
  private val log = LogManager.getLogger(classOf[ItemDAO])

  override def getOne(item: Item): Item = {
    val itemTbl = getHbaseTbl(connection, TABLE_NAME)

    val g = new Get(Item.hbaseRowKey(item))
    g.addFamily(ITEM_INFO_FAM)
    val rs = itemTbl.get(g)
    if (rs.isEmpty) {
      log.warn(s"User ${item.itemId} not found")
    }

    val extractedItem = Item.of(rs)
    itemTbl.close()

    extractedItem
  }

  override def getAll(item: Item): List[Item] = {
    val itemTbl = getHbaseTbl(connection, TABLE_NAME)

    val sc = new Scan
    sc.addFamily(ITEM_INFO_FAM)
    val items = itemTbl.getScanner(sc)
    val builder = List.newBuilder[Item]
    items.forEach(item => {
      builder += item
    })
    val rs = builder.result()
    itemTbl.close()

    rs
  }

  override def addOne(item: Item): Unit = {
    val itemTbl = getHbaseTbl(connection, TABLE_NAME)

    val p = new Put(Item.hbaseRowKey(item))
    p.addColumn(ITEM_INFO_FAM, ITEM_ID_COL, Bytes.toBytes(item.itemId))
    p.addColumn(ITEM_INFO_FAM, ITEM_NAME_COL, Bytes.toBytes(item.name))
    p.addColumn(ITEM_INFO_FAM, ITEM_PRICE_COL, Bytes.toBytes(item.price))
    itemTbl.put(p)
    log.debug(s"Added item ${item.itemId}")

    itemTbl.close()
  }

  override def deleteOne(item: Item): Unit = {
    val itemTbl = getHbaseTbl(connection, TABLE_NAME)

    val d = new Delete(Item.hbaseRowKey(item))
    itemTbl.delete(d)
    log.debug(s"Deleted item ${item.itemId}")

    itemTbl.close()
  }
}
