package utils

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8

import org.lmdbjava.{CursorIterator, Dbi, Txn}
import scalaz.zio.{IO, UIO}
import LMDB._

object Bank {

  def createAccount(tx: UIO[Txn[ByteBuffer]], db: UIO[Dbi[ByteBuffer]], id: String, amount: Double) = {
    putOnLmdb(tx, db, createElement(id), createElement(amount))
  }

  def withdraw(tx: UIO[Txn[ByteBuffer]], db: UIO[Dbi[ByteBuffer]], curs: CursorIterator[ByteBuffer], id: String, amount: Double) = {
    var value: Double = 0
    var key: String = ""
    while (curs.hasNext) {
      val kv = curs.next()
      key = UTF_8.decode(kv.key()).toString
      if (key.equals(id)) {
        value = UTF_8.decode(kv.`val`()).toString.toDouble
        if (amount <= value)
          value = value - amount
      }
    }
    putOnLmdb(tx, db, createElement(id), createElement(value))
  }

  def addToBalance(tx: UIO[Txn[ByteBuffer]], db: UIO[Dbi[ByteBuffer]], curs: CursorIterator[ByteBuffer], id: String, amount: Double) = {
    var value: Double = 0
    var key: String = ""
    while (curs.hasNext) {
      val kv = curs.next()
      key = UTF_8.decode(kv.key()).toString
      if (key.equals(id)) {
        value = UTF_8.decode(kv.`val`()).toString.toDouble
        value = value + amount
      }
    }
    putOnLmdb(tx, db, createElement(id), createElement(value))
  }
}