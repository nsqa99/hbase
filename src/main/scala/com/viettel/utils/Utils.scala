package com.viettel.utils

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Result, Scan, Table}

import scala.io.{BufferedSource, Source}

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

  def loadFromFile[T](filePath: String, constructorFunc: Array[String] => T, delimiter: String = "\\|"): Option[List[T]] = {
    var bufferedSourceOpt: Option[BufferedSource] = None
    val listBuilder = List.newBuilder[T]

    try {
      bufferedSourceOpt = Some(Source.fromFile(filePath))
      val bufferedSource = bufferedSourceOpt.get
      for (line <- bufferedSource.getLines) {
        val data = line.split(delimiter)
        val elem = constructorFunc(data)
        listBuilder += elem
      }

      Some(listBuilder.result())
    } catch {
      case e: Exception =>
        e.printStackTrace()
        None
    } finally {
      if (bufferedSourceOpt.isDefined) {
        bufferedSourceOpt.get.close
      }
    }
  }
}
