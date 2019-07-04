package io.iohk.iodb.issues

import java.io.File

import io.iohk.iodb.LSMStore
import io.iohk.iodb.ByteArrayWrapper
import io.iohk.iodb.TestUtils.{fromLong, randomA}
import scorex.crypto.hash.Blake2b256

object ErgoWalletIssue {
  val insertionsCount = 100000
  val keySize = 32

  def main(args: Array[String]): Unit = {
    val storeDir = new File("tempdir5")
    storeDir.mkdirs();
    val s = new LSMStore(storeDir)
    var lastKey: ByteArrayWrapper = null //= new ByteArrayWrapper(BigInt("14956095050651051591550659499651382088520152618993681999797590944563519175328").toByteArray)
    for (i <- 0 to insertionsCount) {
      val key = ByteArrayWrapper(Blake2b256.hash(i.toString))
      val value = randomA(keySize)
      s.update(
        i,
        Seq.empty,
        Seq((key, value)))
      if (i == insertionsCount - 1) {
        lastKey = key
      }
      if (i % 10000 == 0) {
        println(i)
      }
    }

    s.close()
    var reopenedStore = new LSMStore(storeDir)
    val result = reopenedStore.get(lastKey)
    println(result)
  }
}
