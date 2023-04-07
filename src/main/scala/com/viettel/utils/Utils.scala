package com.viettel.utils

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Table}

/**
 * @author anhnsq@viettel.com.vn
 */
object Utils {
  def getHbaseTbl(connection: Connection, tableName: Array[Byte]): Table = {
    connection.getTable(TableName.valueOf(tableName))
  }
}
