package com.viettel

import com.viettel.conf.HBaseConnectionConf
import com.viettel.dao.{OrderDAO, OrderItemDAO}
import com.viettel.service.{HBaseAdminServiceImpl, OrderServiceImpl}

object Runner {
  def main(args: Array[String]): Unit = {
    val hbaseConn = HBaseConnectionConf.getConnection
    val admin = new HBaseAdminServiceImpl(hbaseConn)

//    val userDAO = new UserDAO(hbaseConn)
    val orderDAO = new OrderDAO(hbaseConn)
    val orderItemDAO = new OrderItemDAO(hbaseConn)

//    val userService = new UserServiceImpl(userDAO, admin)
    val orderService = new OrderServiceImpl(orderDAO, orderItemDAO, admin)

    val rs = orderService.getUserOrdersWithinTimeRange(
      username = "anhnsq",
      startTime = 1677603600000L,
      endTime = 1677862799000L)

    rs.foreach(println)

    hbaseConn.close()
  }
}
