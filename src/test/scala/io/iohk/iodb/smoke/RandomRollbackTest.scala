package io.iohk.iodb.smoke

import io.iohk.iodb.Store._
import io.iohk.iodb.TestUtils._
import io.iohk.iodb.{ByteArrayWrapper, Store}
import org.scalatest.Matchers._

import scala.collection.mutable
import scala.util.Random

/**
  * Randomly inserts data and performs rollback
  */
object RandomRollbackTest {

  val randomSeed = 111
  val loops = 100
  val keepVersions = 100

  val maxRemoveBatchSize = 20

  val maxInsertBatchSize = 200

  def test(store:Store): Unit = {

    val r = new Random(randomSeed)

    def randomBuf: ByteArrayWrapper = {
      val b = new ByteArrayWrapper(32)
      r.nextBytes(b.data)
      b
    }

    val history = new mutable.TreeMap[Long, (Map[K, V], Set[K])]()
    var inserted = Map[K, V]()
    var removed = Set[K]()
    var version = 1L

    //    , keepVersions = keepVersions)
    //      maxJournalEntryCount = 1000, splitSize = 1000, maxFileSize = 64000, executor = null)

    for (i <- 0 until loops) {
      //randomly switch between actions
      val a = r.nextInt(100)
      if (a < 3 && history.size > 2) {
        //perform rollback
        version = r.shuffle(history.keys.dropRight(1)).head
        inserted = history(version)._1
        removed = history(version)._2

        //remove unused items from history
        history.keys.foreach(v => if (v > version) history.remove(v))
        store.rollback(fromLong(version))
      } else if (a < 6 && history.size > 4) {
        //perform cleanup
        store.clean(keepVersions)
      } else {
        //insert data
        val toUpdate =
          (0 until r.nextInt(maxInsertBatchSize))
            .map(i => (randomBuf, randomBuf))
            .filter(a => !removed.contains(a._1))
            .toMap

        val toRemove = r.shuffle(inserted.keys)
          .take(r.nextInt(maxRemoveBatchSize))
          .filter(!toUpdate.contains(_))

        version += 1
        store.update(versionID = fromLong(version), toRemove = toRemove, toUpdate = toUpdate)

        for (k <- toRemove)
          assert(store.get(k) == None)

        inserted = inserted.++(toUpdate).--(toRemove)
        removed ++= toRemove
        //check this version is biggest
        assert(history.keys.forall(_ < version))
        history.put(version, (inserted, removed))
        //cut history so it has maximally 'keepVersions' entries
        history.keys
          .take(Math.max(0, history.size - keepVersions))
          .toBuffer
          .foreach(history.remove(_))
      }

      assert(Some(fromLong(version)) == store.lastVersionID)

      //check current state matches store
      for ((k, v) <- inserted) {
        assert(store.get(k) == Some(v))
      }
      for (k <- removed) {
        assert(store.get(k) == None)
      }
      store.verify()

      val versionsFromStore = store.rollbackVersions().toBuffer.sorted.takeRight(history.size)
      val versionsFromHistory = history.keySet.map(fromLong(_)).toBuffer

      versionsFromStore shouldBe versionsFromHistory

      val getAll = store.getAll()
      val b1 = getAll.toBuffer.sortBy[ByteArrayWrapper](_._1).toBuffer
      val b2 = inserted.toBuffer.sortBy[ByteArrayWrapper](_._1).toBuffer
      b1.size shouldBe b2.size
      b1 shouldBe b2

    }
    store.close()
  }

}
