package com.viettel

import com.viettel.conf.HBaseConnectionConf
import com.viettel.dao.{OrderDAO, OrderItemDAO, UserDAO}
import com.viettel.service.{HBaseAdminServiceImpl, LoadServiceImpl, OrderServiceImpl, UserServiceImpl}
import org.slf4j.LoggerFactory

/**
 * @author anhnsq@viettel.com.vn
 */
object PrepareDataRunner {
  private val log = LoggerFactory.getLogger("PrepareData")

  def main(args: Array[String]): Unit = {
    val hbaseConn = HBaseConnectionConf.getConnection
    val admin = new HBaseAdminServiceImpl(hbaseConn)

    val userDAO = new UserDAO(hbaseConn)
    val orderDAO = new OrderDAO(hbaseConn)
    val orderItemDAO = new OrderItemDAO(hbaseConn)

    val userService = new UserServiceImpl(userDAO, admin)
    val orderService = new OrderServiceImpl(orderDAO, orderItemDAO, admin)

    val loadService = new LoadServiceImpl
    val (listUsers, listOrders, listOrderItems) = loadService.loadDataFromFiles()
    log.info("Creating users table...")
    userService.createUserTable()
    log.info("Done")
    log.info("Creating orders table...")
    orderService.createOrderTable()
    log.info("Done")
    log.info("Creating order_items table...")
    orderService.createOrderItemTable()
    log.info("Done")

    log.info("Inserting users...")
    userService.insertUsers(listUsers)
    log.info("Done")
    log.info("Inserting orders...")
    orderService.insertOrders(listOrders)
    log.info("Done")
    log.info("Inserting order_items...")
    orderService.insertOrderItems(listOrderItems)
    log.info("Done")

    hbaseConn.close()
  }
}
