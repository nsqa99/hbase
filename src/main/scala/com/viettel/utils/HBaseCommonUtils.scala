package com.viettel.utils

/**
 * @author anhnsq@viettel.com.vn
 */
trait HBaseCommonUtils[T] {
  def hbaseRowKey(elem: T): Array[Byte]
}
