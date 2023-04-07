package com.viettel.utils

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Result, Scan, Table}

/**
 * @author anhnsq@viettel.com.vn
 */
object Utils {
  def getHbaseTbl(connection: Connection, tableName: Array[Byte]): Table = {
    connection.getTable(TableName.valueOf(tableName))
  }

  def getAll[T](connection: Connection, tableName: Array[Byte], columnFamily: Array[Byte], constructor: Result => T): List[T] = {
    val table = getHbaseTbl(connection, tableName)

    val sc = new Scan
    sc.addFamily(columnFamily)
    val results = table.getScanner(sc)

    val builder = List.newBuilder[T]
    results.forEach(elem => {
      builder += constructor(elem)
    })
    val rs = builder.result()
    table.close()

    rs
  }
}
