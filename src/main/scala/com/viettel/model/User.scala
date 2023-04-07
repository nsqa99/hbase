package com.viettel.model


import com.viettel.utils.HBaseCommonUtils
import org.apache.hadoop.hbase.client.{Connection, Result, Table}
import org.apache.hadoop.hbase.util.Bytes

/**
 * @author anhnsq@viettel.com.vn
 */
class User private (var username: String, var fullName: String, var city: String, var age: Int) {
}

object User extends HBaseCommonUtils[User] {
  val TABLE_NAME: Array[Byte] = Bytes.toBytes("users")
  val INFO_FAM: Array[Byte] = Bytes.toBytes("info")
  val USERNAME_COL: Array[Byte] = Bytes.toBytes("username")
  val FULL_NAME_COL: Array[Byte] = Bytes.toBytes("full_name")
  val CITY_COL: Array[Byte] = Bytes.toBytes("city")
  val AGE_COL: Array[Byte] = Bytes.toBytes("age")

  def of(rs: Result): User = {
    val username = rs.getValue(INFO_FAM, USERNAME_COL)
    val fullName = rs.getValue(INFO_FAM, FULL_NAME_COL)
    val city = rs.getValue(INFO_FAM, CITY_COL)
    val age = rs.getValue(INFO_FAM, AGE_COL)

    new User(Bytes.toString(username), Bytes.toString(fullName), Bytes.toString(city), Bytes.toInt(age))
  }

  override def hbaseRowKey(user: User): Array[Byte] = {
    Bytes.toBytes(s"${user.city}+${user.username}")
  }
}
