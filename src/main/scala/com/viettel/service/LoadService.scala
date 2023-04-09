package com.viettel.service

import com.viettel.model.{Item, Order, OrderItem, User}
import com.viettel.utils.Utils.loadFromFile

class LoadService {
  def loadDataFromFiles(): (List[User], List[Order], List[OrderItem]) = {
    var listUsers = List[User]()
    var listOrders = List[Order]()
    var listItems = List[Item]()
    var listOrderItems = List[OrderItem]()

    val usersOpt: Option[List[User]] = loadFromFile[User]("src/main/resources/users.txt", User.of)
    val ordersOpt: Option[List[Order]] = loadFromFile[Order]("src/main/resources/orders.txt", Order.of)
    val itemsOpt: Option[List[Item]] = loadFromFile[Item]("src/main/resources/items.txt", Item.of)
    val orderItemsOpt: Option[List[OrderItem]] = loadFromFile[OrderItem]("src/main/resources/order_items.txt", OrderItem.of)

    if (usersOpt.isDefined) {
      listUsers = usersOpt.get
    }
    if (ordersOpt.isDefined) {
      listOrders = ordersOpt.get
    }
    if (itemsOpt.isDefined) {
      listItems = itemsOpt.get
    }
    if (orderItemsOpt.isDefined) {
      val itemMap = listItems.map(item => (item.itemId, item)).toMap
      listOrderItems = orderItemsOpt.get
      listOrderItems.foreach(ordItem => {
        val itemId = ordItem.item.itemId
        if (itemMap.contains(itemId)) {
          ordItem.item = itemMap(itemId)
        }
      })
    }

    (listUsers, listOrders, listOrderItems)
  }
}
