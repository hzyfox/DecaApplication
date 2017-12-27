package microBenchmark

/**
  * create with microBenchmark
  * USER: husterfox
  */

import java.util


/**
  * Created by 路 on 2016/4/8.
  */
abstract class UnsafeMap(val kvMaxCount: Int, val kSize: Int, val vSize: Int) {

  import microBenchmark.UnsafeMap._

  protected val baseAddress = UNSAFE.allocateMemory(kvMaxCount.toLong * (kSize + vSize))
  protected var curAddress = baseAddress
  protected val elementNotExist: Int = -2
  protected var kvCount = 0
  protected val loadFactor = 0.75
  protected val maxProbes = 5000
  protected val capacity = nextPowerOf2((kvMaxCount / loadFactor).ceil.toLong).toInt
  protected val mask = capacity - 1
  protected var pointers = new Array[Long](capacity)
  for (i <- pointers.indices) {
    pointers(i) = elementNotExist
  }

  def numElements = kvCount

  def address = baseAddress

  def clear(): Unit = {
    var i = 0
    while (i < kvCount) {
      pointers(i) = 0L
      i += 1
    }
    kvCount = 0
    curAddress = baseAddress
  }

  def free(): Unit = {
    kvCount = 0
    pointers = null
    UNSAFE.freeMemory(baseAddress)
    curAddress = 0L
  }
}

class IntIntArrayMap(keysize: Long, arrayListSize: Long) {

  import microBenchmark.UnsafeMap._

  /*for every IntIntArray element:  the 0th element is rank and the 1th is the len,the 2th is key and  then the arraylist*/
  protected val address: Long = UNSAFE.allocateMemory(keysize * 16 + arrayListSize * 4)
  protected var curAddress: Long = address
  protected val loadFactor = 0.75
  protected val initPointers: Int = -2
  val keyNotFound = -1
  protected val maxProbes = 5000
  protected val capacity = nextPowerOf2((keysize / loadFactor).ceil.toLong).toInt
  protected val mask = capacity - 1
  protected var pointers = new Array[Long](capacity)
  for (i <- pointers.indices) {
    pointers(i) = initPointers
  }

  def getKeyAddress(key: Int): Long = {
    var pos = key & mask
    var step = 1
    while (step < maxProbes) {
      if (pointers(pos) == initPointers) {
        return keyNotFound
      } else {
        val pointer = pointers(pos)
        if (UNSAFE.getInt(pointer + 12) == key) {
          return pointer
        }
      }
      pos = (pos + step) & mask
      step += 1
    }
    keyNotFound
  }

  def free(): Unit = {
      pointers = null
      UNSAFE.freeMemory(address)
      curAddress = 0L
  }

  def putKV(key: Int, valueList: util.ArrayList[Integer]): Unit = {
    var pos = key & mask
    var step = 1
    while (step < maxProbes) {
      if (pointers(pos) == initPointers) {
        pointers(pos) = curAddress
        UNSAFE.putDouble(curAddress, -1.0)
        curAddress += 8
        if (valueList != null) {
          UNSAFE.putInt(curAddress, valueList.size())
        } else {
          UNSAFE.putInt(curAddress, 0)
        }
        curAddress += 4
        UNSAFE.putInt(curAddress, key)
        curAddress += 4
        var i = 0;
        val vl = valueList.size()
        while (i < vl) {
          UNSAFE.putInt(curAddress, valueList.get(i))
          i += 1
          curAddress += 4
        }
        return
      } else {
        pos = (pos + step) & mask
        step += 1
      }
    }
    throw new UnsupportedOperationException
  }

}

class IntDoubleMap(kvMaxCount: Int)
  extends UnsafeMap(kvMaxCount, 4, 8) {
  val defaultValue: Double = -1.0

  def get(key: Int): Double = {
    var pos = key & mask
    var step = 1
    while (step < maxProbes) {
      if (pointers(pos) == elementNotExist) {
        return defaultValue
      } else {
        val pointer = pointers(pos)
        if (UNSAFE.getInt(pointer) == key)
          return UNSAFE.getDouble(pointer + kSize)
      }
      pos = (pos + step) & mask
      step += 1
    }
    throw new UnsupportedOperationException
  }

  def put(key: Int, value: Double): Unit = {
    var pos = key & mask
    var step = 1
    while (step < maxProbes) {
      if (pointers(pos) == elementNotExist) {
        insert(pos, key, value)
        return
      } else {
        val pointer = pointers(pos)
        if (UNSAFE.getInt(pointer) == key) {
          UNSAFE.putDouble(pointer + kSize, value)
          return
        }
      }
      pos = (pos + step) & mask
      step += 1
    }
    throw new UnsupportedOperationException
  }

  private def insert(pos: Int, key: Int, value: Double) {
    //    println(s"put at $pos")
    if (kvCount == kvMaxCount)
      throw new UnsupportedOperationException
    kvCount += 1
    pointers(pos) = curAddress
    UNSAFE.putInt(curAddress, key)
    curAddress += kSize
    UNSAFE.putDouble(curAddress, value)
    curAddress += vSize
  }

  def orderGetKey(index: Int): Int = {
    UNSAFE.getInt(address + 12 * index)
  }

  def orderGetValue(index: Int): Double = {
    UNSAFE.getDouble(address + 12 * index + 4)
  }

  override def toString: String = {
    var address = baseAddress
    val stringBuilder = new StringBuilder
    var i = 0
    while (i < kvCount) {
      val key = UNSAFE.getInt(address)
      address += kSize
      stringBuilder.append(key + ": ")
      val value = UNSAFE.getDouble(address)
      address += vSize
      stringBuilder.append(value + "; ")
      i += 1
    }
    stringBuilder.toString()
  }
}


class IntLongMap(kvMaxCount: Int)
  extends UnsafeMap(kvMaxCount, 4, 8) {
  val defaultValue: Long = -1l

  def get(key: Int): Long = {
    var pos = key & mask
    var step = 1
    while (step < maxProbes) {
      if (pointers(pos) == elementNotExist) {
        return defaultValue
      } else {
        val pointer = pointers(pos)
        if (UNSAFE.getInt(pointer) == key)
          return UNSAFE.getLong(pointer + kSize)
      }
      pos = (pos + step) & mask
      step += 1
    }
    throw new UnsupportedOperationException
  }


  def put(key: Int, intintArrayRecorder: IntIntArrayMap): Unit = {
    var pos = key & mask
    var step = 1
    while (step < maxProbes) {
      if (pointers(pos) == elementNotExist) {
        insert(pos, key, intintArrayRecorder)
        return
      } else {
        pos = (pos + step) & mask
        step += 1
      }
    }

  }

  def insert(pos: Int, key: Int, intintArrayRecorder: IntIntArrayMap): Unit = {
    kvCount += 1
    pointers(pos) = curAddress
    UNSAFE.putInt(curAddress, key)
    curAddress += kSize
    /*long指向IntIntArrayMap中的结构*/
    val keyAddress = intintArrayRecorder.getKeyAddress(key)
    if (keyAddress == intintArrayRecorder.keyNotFound) {
      throw new UnsupportedOperationException
    }
    UNSAFE.putLong(curAddress, keyAddress)
    curAddress += vSize
  }

  /*
  获取Intlong数组的中的int==》key
   */
  def orderGetKey(index: Int): Int = {
    UNSAFE.getInt(address + 12 * index)
  }

  /*
 获取Intlong数组的中的long ==》address
  */
  def orderGetValue(index: Int): Long = {
    UNSAFE.getLong(address + 12 * index + 4)
  }

  /*
  往long指向的地址put rank
   */
  def putPairDouble(vladdress: Long, value: Double): Unit = {
    UNSAFE.putDouble(vladdress, value)
  }

  /*
  get long指向地址的rank
   */
  def getPairDouble(vlAddress: Long): Double = {
    UNSAFE.getDouble(vlAddress)
  }

  /*
  获取long指向地址的arraylist长度
   */
  def getPairVLLength(vlAddress: Long): Int = {
    UNSAFE.getInt(vlAddress + 8)
  }

  /*
  按arraylist下标获取long指向地址中arraylist的元素
   */
  def getPairVlValue(vlAddress: Long, index: Int): Int = {
    UNSAFE.getInt(vlAddress + 16 + index * 4)
  }

  override def toString: String = {
    var address = baseAddress
    val stringBuilder = new StringBuilder
    var i = 0
    while (i < kvCount) {
      val key = UNSAFE.getInt(address)
      address += kSize
      stringBuilder.append(key + ": ")
      val value = UNSAFE.getLong(address)
      address += vSize
      stringBuilder.append(value + "; ")
      i += 1
    }
    stringBuilder.toString()
  }
}

class IntPairMap(kvMaxCount: Int)
  extends UnsafeMap(kvMaxCount, 4, 16) {

  val v1Size = 8
  val v2Size = 8

  val defaultValue1 = -1.0
  val defaultValue2 = -1L

  def get1(key: Int): Double = {
    var pos = key & mask
    var step = 1
    while (step < maxProbes) {
      if (pointers(pos) == elementNotExist) {
        return defaultValue1
      } else {
        val pointer = pointers(pos)
        if (UNSAFE.getInt(pointer) == key)
          return UNSAFE.getDouble(pointer + kSize)
      }
      pos = (pos + step) & mask
      step += 1
    }
    throw new UnsupportedOperationException
  }

  def get2(key: Int): Long = {
    var pos = key & mask
    var step = 1
    while (step < maxProbes) {
      if (pointers(pos) == elementNotExist) {
        return defaultValue2
      } else {
        val pointer = pointers(pos)
        if (UNSAFE.getInt(pointer) == key)
          return UNSAFE.getLong(pointer + kSize + v1Size)
      }
      pos = (pos + step) & mask
      step += 1
    }
    throw new UnsupportedOperationException
  }

  def put1(key: Int, value1: Double): Unit = {
    var pos = key & mask
    var step = 1
    while (step < maxProbes) {
      if (pointers(pos) == elementNotExist) {
        insert(pos, key, value1, defaultValue2)
        return
      } else {
        val pointer = pointers(pos)
        if (UNSAFE.getInt(pointer) == key) {
          UNSAFE.putDouble(pointer + kSize, value1)
          return
        }
      }
      pos = (pos + step) & mask
      step += 1
    }
    throw new UnsupportedOperationException
  }

  def put2(key: Int, value2: Long): Unit = {
    var pos = key & mask
    var step = 1
    while (step < maxProbes) {
      if (pointers(pos) == elementNotExist) {
        insert(pos, key, defaultValue2, value2)
        return
      } else {
        val pointer = pointers(pos)
        if (UNSAFE.getInt(pointer) == key) {
          UNSAFE.putLong(pointer + kSize + v1Size, value2)
          return
        }
      }
      pos = (pos + step) & mask
      step += 1
    }
    throw new UnsupportedOperationException
  }

  def put(key: Int, value1: Double, value2: Long): Unit = {
    var pos = key & mask
    var step = 1
    while (step < maxProbes) {
      if (pointers(pos) == elementNotExist) {
        insert(pos, key, value1, value2)
        return
      } else {
        val pointer = pointers(pos)
        if (UNSAFE.getInt(pointer) == key) {
          UNSAFE.putDouble(pointer + kSize, value1)
          UNSAFE.putLong(pointer + kSize + v1Size, value2)
          return
        }
      }
      pos = (pos + step) & mask
      step += 1
    }
    throw new UnsupportedOperationException
  }

  private def insert(pos: Int, key: Int, value1: Double, value2: Long) {
    if (kvCount == kvMaxCount)
      throw new UnsupportedOperationException
    kvCount += 1
    pointers(pos) = curAddress
    UNSAFE.putInt(curAddress, key)
    curAddress += kSize
    UNSAFE.putDouble(curAddress, value1)
    curAddress += v1Size
    UNSAFE.putLong(curAddress, value2)
    curAddress += v2Size
  }
}


object UnsafeMap {
  //  final val UNSAFE = {
  //    val unsafeField = classOf[sun.misc.Unsafe].getDeclaredField("theUnsafe")
  //    unsafeField.setAccessible(true)
  //    unsafeField.get().asInstanceOf[sun.misc.Unsafe]
  //  }

  def nextPowerOf2(num: Long): Long = {
    val highBit: Long = java.lang.Long.highestOneBit(num)
    if (highBit == num) num else highBit << 1
  }

  def main(args: Array[String]): Unit = {
    val map1 = new IntDoubleMap(100)
    map1.put(1, 1.0)
    map1.put(1, 1.1)
    map1.put(2, 2.0)
    map1.put(3, 3.0)
    map1.put(4, 4.0)
    map1.put(2, 2.1)

    println(s"1: ${map1.get(1)}")

    var address = map1.address
    var i = 0
    while (i < map1.numElements) {
      val key = UNSAFE.getInt(address)
      address += map1.kSize
      val value = UNSAFE.getDouble(address)
      address += map1.vSize
      println(s"$key: $value")
      i += 1
    }

    println(s"elements: ${map1.numElements}")


    val map2 = new IntPairMap(100)
    map2.put(1, 1.0, 1000L)
    map2.put(1, 1.1, 1111L)
    map2.put(2, 2.0, 2000L)
    map2.put(3, 3.0, 3000L)
    map2.put(4, 4.0, 4000L)
    map2.put(2, 2.1, 2222L)
    map2.put1(5, 5.0)
    map2.put2(6, 6000L)
    map2.put2(5, 5555L)
    map2.put1(6, 6.1)


    println(s"1: ${map2.get1(1)},${map2.get2(1)}")
    println(s"5: ${map2.get1(5)},${map2.get2(5)}")
    println(s"6: ${map2.get1(6)},${map2.get2(6)}")

    address = map2.address
    i = 0
    while (i < map2.numElements) {
      val key = UNSAFE.getInt(address)
      address += map2.kSize
      val value1 = UNSAFE.getDouble(address)
      address += map2.v1Size
      val value2 = UNSAFE.getLong(address)
      address += map2.v2Size
      println(s"$key: $value1,$value2")
      i += 1
    }

    println(s"elements: ${map2.numElements}")
  }
}

