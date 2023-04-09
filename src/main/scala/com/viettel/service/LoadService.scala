package com.viettel.service

import com.viettel.model.{Order, OrderItem, User}

trait LoadService {
  def loadDataFromFiles(): (List[User], List[Order], List[OrderItem])
}
