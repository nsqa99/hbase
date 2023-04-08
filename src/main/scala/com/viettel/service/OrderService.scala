package com.viettel.service

import com.viettel.model.Order

trait OrderService {
  def getUserOrdersWithinTimeRange(username: String, startTime: Long, endTime: Long): List[Order]
}
