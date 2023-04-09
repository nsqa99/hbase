package com.viettel.service
import com.viettel.dao.UserDAO
import com.viettel.model.User
import org.apache.hadoop.hbase.TableName

class UserServiceImpl(userDAO: UserDAO, admin: HBaseAdminService) extends UserService {
  override def insertUsers(users: List[User]): Unit = {
    users.foreach(userDAO.addOne)
  }

  override def createUserTable(): Unit = {
    admin.createTable(TableName.valueOf(User.TABLE_NAME), User.INFO_FAM)
  }
}
