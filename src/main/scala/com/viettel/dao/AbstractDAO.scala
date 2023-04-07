package com.viettel.dao

/**
 * @author anhnsq@viettel.com.vn
 */
trait AbstractDAO[T] {
  def getOne(item: T): T
  def getAll(item: T): List[T]
  def addOne(item: T): Unit
  def deleteOne(item: T): Unit
}
