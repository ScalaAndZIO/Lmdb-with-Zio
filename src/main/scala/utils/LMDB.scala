package utils
import java.io.File
import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocateDirect
import java.nio.charset.StandardCharsets.UTF_8

import org.lmdbjava.DbiFlags.MDB_CREATE
import org.lmdbjava.Env.{AlreadyClosedException, Builder, create}
import org.lmdbjava.EnvFlags.MDB_NOSUBDIR
import org.lmdbjava._
import scalaz.zio._
import scalaz.zio.console.{Console, putStrLn}
import org.lmdbjava.{CursorIterator, Dbi, Txn}

object LMDB {

  def createEnv(): ZIO[Any, Throwable, Builder[ByteBuffer]] = for {
    environment <- IO.succeed(create())
  } yield (environment)

  def setSizeEnv2(size: Int, env: Builder[ByteBuffer]): ZIO[Any, Throwable, Any] =
    IO.effect(env.setMapSize(size)).catchAll(e => IO.succeed(e.getMessage))

  def openEnv(env: ZIO[Any, Throwable, Builder[ByteBuffer]], lmdbFile: File, flag: EnvFlags): ZIO[Console, Throwable, Any]  = for {
    environment <- env
    openedEnv <- IO.effect(environment.open(lmdbFile, flag)).catchAll(e =>IO.succeed("error in file " + e.getMessage))
  } yield openedEnv


  def openDb(env: ZIO[Any, Throwable, Env[ByteBuffer]], lmdbName: String, flag: DbiFlags): ZIO[Console, Throwable, Any] = for {
    environment <- env
    db <- IO.effect(environment.openDbi(lmdbName, flag)).catchAll(e => IO.succeed(e.getMessage))
  } yield db

  def createWriteTx(env: UIO[Env[ByteBuffer]]): ZIO[Console, Throwable, Any] = for {
    environment <- env
    txnWrite <- IO.effect(environment.txnWrite()).catchAll(e => IO.succeed(e.getMessage))
  } yield txnWrite

  def putOnLmdb(tx: UIO[Txn[ByteBuffer]], db: UIO[Dbi[ByteBuffer]], key: ByteBuffer, value: ByteBuffer): ZIO[Console, Throwable, Any] = for {
    db2 <- db
    tx2 <- tx
    put <- IO.effect(db2.put(tx2, key, value)).catchAll(e => putStrLn(e.getMessage))
  } yield put


  def commitToDb(tx: UIO[Txn[ByteBuffer]]) = for {
    txn <- tx
    commit <- IO.effect(txn.commit()).catchAll(e => putStrLn(e.getMessage))
  } yield commit


  def closeTxn(txn: Txn[ByteBuffer]): ZIO[Console, Throwable, Any] = {
    IO.effect(txn.close()).catchAll(e => putStrLn(e.getMessage))
  }

  def createReadTx(env: UIO[Env[ByteBuffer]]): ZIO[Console, Throwable, Any] = for {
    environment <- env
    txnRead <- IO.effect(environment.txnRead()).catchAll(e => putStrLn(e.getMessage))
  } yield txnRead

  def createElement[A](value: A): ByteBuffer = {
    val bb = allocateDirect(200)
    bb.put(value.toString.getBytes(UTF_8)).flip
    bb
  }

  def readFromDb(txn: UIO[Txn[ByteBuffer]], dbi: UIO[Dbi[ByteBuffer]]) = for {
    tx <- txn
    db <- dbi
    cur <- IO.effect(db.iterate(tx, KeyRange.all[ByteBuffer]())).catchAll(e => putStrLn(e.getMessage))
  } yield cur

  def hasNextCur(c: CursorIterator[ByteBuffer]) = for {
    has <- IO.succeed(c.hasNext)
  } yield (IO.succeed(has))

  def printValues(c: CursorIterator[ByteBuffer]): ZIO[Console, Nothing, CursorIterator[ByteBuffer]] = for {
    flag <- hasNextCur(c)
    hasNext <- flag
    loop <- if (hasNext) {
      val kv = c.next()
      putStrLn(UTF_8.decode(kv.key()).toString + "  " + UTF_8.decode(kv.`val`()).toString).const(true)
    }
    else putStrLn("").const(false)
    _ <- if (loop) printValues(c) else IO.succeed("")
  } yield (c)

}