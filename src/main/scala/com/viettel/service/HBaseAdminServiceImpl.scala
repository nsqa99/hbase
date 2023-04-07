package com.viettel.service

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Admin, ColumnFamilyDescriptor, ColumnFamilyDescriptorBuilder, Connection, TableDescriptorBuilder}
import org.apache.log4j.LogManager

import java.io.IOException
import java.util

/**
 * @author anhnsq@viettel.com.vn
 */
class HBaseAdminServiceImpl(connection: Connection) extends HBaseAdminService {
  private val log = LogManager.getLogger(classOf[HBaseAdminServiceImpl])

  override def createTable(tableName: TableName, columnFamilies: Array[Byte]*): Unit = {
    var adminOpt: Option[Admin] = None
    try {
      adminOpt = Some(connection.getAdmin)
      val tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName)
      val listColumnFamilies = new util.ArrayList[ColumnFamilyDescriptor]()
      columnFamilies.foreach(colFam => {
        listColumnFamilies.add(ColumnFamilyDescriptorBuilder.newBuilder(colFam).build())
      })

      tableDescriptorBuilder.setColumnFamilies(listColumnFamilies)
      val tableDesc = tableDescriptorBuilder.build()
      val admin = adminOpt.get
      if (admin.tableExists(tableName)) {
        log.debug(s"Table ${tableName.getNameAsString} already exists. Overwriting...")
        admin.disableTable(tableName)
        admin.deleteTable(tableName)
      }

      admin.createTable(tableDesc)
      log.debug(s"Table ${tableName.getNameAsString} created")
    } catch {
      case e: IOException => log.error("Error when creating table", e)
    } finally {
      if (adminOpt.isDefined) adminOpt.get.close()
    }
  }

  override def addColumnFamily(tableName: TableName, columnFamily: Array[Byte]): Unit = {
    var adminOpt: Option[Admin] = None
    try {
      adminOpt = Some(connection.getAdmin)
      val admin = adminOpt.get
      if (!admin.tableExists(tableName)) {
        log.error(s"Table ${tableName.getNameAsString} is not exists")
        return
      }

      admin.disableTable(tableName)
      admin.addColumnFamily(tableName, ColumnFamilyDescriptorBuilder.newBuilder(columnFamily).build())
      admin.enableTable(tableName)
    } catch {
      case e: IOException => log.error("Error when add column family", e)
    } finally {
      if (adminOpt.isDefined) adminOpt.get.close()
    }
  }
}
