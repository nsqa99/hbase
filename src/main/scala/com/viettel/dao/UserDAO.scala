package com.viettel.dao

import com.viettel.model.User
import com.viettel.utils.Utils
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.LogManager

/**
 * @author anhnsq@viettel.com.vn
 */
class UserDAO(connection: Connection) extends BaseDAO[User] {

  import com.viettel.model.User._

  private val log = LogManager.getLogger(classOf[UserDAO])

  private def getHbaseTbl(connection: Connection, tableName: Array[Byte]): Table = {
    connection.getTable(TableName.valueOf(tableName))
  }

  override def getOne(user: User): User = {
    val userTbl = getHbaseTbl(connection, TABLE_NAME)

    val g = new Get(User.hbaseRowKey(user))
    g.addFamily(INFO_FAM)
    val rs = userTbl.get(g)
    if (rs.isEmpty) {
      log.warn(s"User ${user.username} not found")
    }

    val extractedUser = User.of(rs)
    userTbl.close()

    extractedUser
  }

  override def getAll(user: User): List[User] = {
    Utils.getAll[User](connection = connection, tableName = TABLE_NAME, columnFamily = INFO_FAM, constructor = User.of)
  }

  override def addOne(user: User): Unit = {
    val userTbl = getHbaseTbl(connection, TABLE_NAME)

    val p = new Put(User.hbaseRowKey(user))
    p.addColumn(INFO_FAM, USERNAME_COL, Bytes.toBytes(user.username))
    p.addColumn(INFO_FAM, FULL_NAME_COL, Bytes.toBytes(user.fullName))
    p.addColumn(INFO_FAM, CITY_COL, Bytes.toBytes(user.city))
    p.addColumn(INFO_FAM, AGE_COL, Bytes.toBytes(user.age))
    userTbl.put(p)
    log.debug(s"Added user ${user.username}")

    userTbl.close()
  }

  override def deleteOne(user: User): Unit = {
    val userTbl = getHbaseTbl(connection, TABLE_NAME)

    val d = new Delete(User.hbaseRowKey(user))
    userTbl.delete(d)
    log.debug(s"Deleted user ${user.username}")

    userTbl.close()
  }
}
