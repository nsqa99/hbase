package com.viettel.service

import com.viettel.model.User

trait UserService {
  def createUserTable(): Unit
  def insertUsers(users: List[User]): Unit
}
