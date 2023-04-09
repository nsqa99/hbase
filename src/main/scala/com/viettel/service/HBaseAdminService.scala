package com.viettel.service

import org.apache.hadoop.hbase.TableName

/**
 * @author anhnsq@viettel.com.vn
 */
trait HBaseAdminService {
  def createTable(tableName: TableName, columnFamilies: Array[Byte]*): Unit
  def addColumnFamily(tableName: TableName, columnFamily: Array[Byte]): Unit
}
