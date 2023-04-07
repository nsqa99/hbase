package com.viettel.conf

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

/**
 * @author anhnsq@viettel.com.vn
 */
object HBaseConnectionConf {
  def getConnection: Connection = {
    val config: Configuration = HBaseConfiguration.create()
    //Add any necessary configuration files (hbase-site.xml, core-site.xml)
    config.addResource(new Path("/home/anhnsq/apps/hbase-2.4.16/hbase-site.xml"))
    config.addResource(new Path("/home/anhnsq/apps/hadoop-3.3.3/core-site.xml"))
    ConnectionFactory.createConnection(config)
  }
}
