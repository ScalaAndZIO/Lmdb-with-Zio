package test

import java.io.File
import java.nio.ByteBuffer

import org.lmdbjava.DbiFlags.MDB_CREATE
import org.lmdbjava.Env.Builder
import org.lmdbjava.EnvFlags.MDB_NOSUBDIR
import org.lmdbjava.{CursorIterator, Dbi, Txn, _}
import scalaz.zio._
import scalaz.zio.console.putStrLn
import utils.Bank._
import utils.LMDB._

object BankTest extends App {

  def run(args: List[String]) =
    myApp.fold(_ => 1, _ => 0)

  val myApp =
    for {
      env <- createEnv()
      env3 <- setSizeEnv2(10485760, env)
      environment <- env3 match {
        case builder: Builder[ByteBuffer] => IO.effect(builder.setMaxDbs(1)).catchAll(e => putStrLn(e.getMessage))
        case error: String => IO.succeed(error.toString)
      }
      openedEnv <- environment match {
        case builder: Builder[ByteBuffer] => openEnv(IO.succeed(builder), new File("bankFile.txt"), MDB_NOSUBDIR)
        case er: String => IO.succeed(er)
      }
      openedDb <- openedEnv match {
        case env: Env[ByteBuffer] => openDb(IO.succeed(env), "bank db ", MDB_CREATE)
        case error: String => IO.succeed(error)
      }

      writeTx <- openedEnv match {
        case env: Env[ByteBuffer] => createWriteTx(IO.succeed(env))
        case er: String => IO.succeed(er)
      }

      account <- (openedDb, writeTx) match {
        case (dbi: Dbi[ByteBuffer], writeTx: Txn[ByteBuffer]) => createAccount(IO.succeed(writeTx), IO.succeed(dbi), "id", 2000.0)
        case (_, _) => IO.succeed("there is error  both in dbi and transaction ")
      }

      _ <- writeTx match {
        case tx: Txn[ByteBuffer] => commitToDb(IO.succeed(tx))
        case er: String => putStrLn(er)
      }

      readTx <- openedEnv match {
        case env: Env[ByteBuffer] => createReadTx(IO.succeed(env))
        case er => putStrLn(er.toString)
      }

      cursor <- (openedDb, readTx) match {
        case (dbi: Dbi[ByteBuffer], tx: Txn[ByteBuffer]) => readFromDb(IO.succeed(tx), IO.succeed(dbi))
        case er => IO.succeed(er.toString)
      }
      _ <- cursor match {
        case cur: CursorIterator[ByteBuffer] => printValues(cur)
        case _ => putStrLn("error in read ")
      }

//      _ <- readTx match {
//        case txn: Txn[ByteBuffer] => closeTxn(txn)
//        case er => putStrLn(er.toString)
//      }

      writeTx2 <- openedEnv match {
        case env: Env[ByteBuffer] => createWriteTx(IO.succeed(env))
        case er: String => IO.succeed(er)
      }

      cursor2 <- (openedDb, readTx) match {
        case (dbi: Dbi[ByteBuffer], tx: Txn[ByteBuffer]) => readFromDb(IO.succeed(tx), IO.succeed(dbi))
        case er => IO.succeed(er.toString)
      }

      _ <- (writeTx2, cursor2, openedDb) match {
        case (wtx: Txn[ByteBuffer], c: CursorIterator[ByteBuffer], dbi: Dbi[ByteBuffer]) => withdraw(IO.succeed(wtx), IO.succeed(dbi), c, "id", 500)
        case _ => putStrLn("error while withdraw")
      }

      _ <- cursor2 match {
        case cur: CursorIterator[ByteBuffer] => printValues(cur)
        case _ => putStrLn("error in read ")
      }

    } yield ()
}