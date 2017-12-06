package microBenchmark

/**
  * create with microBenchmark
  * USER: husterfox
  */

import java.util
import java.util.ArrayList


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
  for(i<-pointers.indices){
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

class IntIntArrayMap(size: Long) {

  protected val address = UNSAFE.allocateMemory(size * 4)
  protected var curAddress: Long = address
  protected var scanAddress: Long = address
  protected val lengths = new util.ArrayList[Integer]()

  def resetScanAddress(): Unit = {
    this.scanAddress = address
  }

  def getLengths(): util.ArrayList[Integer] = {
    this.lengths
  }

  def putKV(key: Int, valueList: util.ArrayList[Integer]): Unit = {
    UNSAFE.putInt(curAddress, key)
    curAddress += 4
    var i = 0;
    val vl = valueList.size()
    while (i < vl) {
      UNSAFE.putInt(curAddress, valueList.get(i))
      i += 1
      curAddress += 4
    }
    lengths.add(valueList.size())
  }

  def getkey(): Int = {
    scanAddress += 4
    UNSAFE.getInt(scanAddress - 4)
  }

  def getValue(index: Int, length: Long): Int = {
    val r = UNSAFE.getInt(scanAddress + index * 4)
    if (index == (length - 1)) {
      scanAddress += length * 4
    }
    r
  }
}

class IntDoubleMap(kvMaxCount: Int)
  extends UnsafeMap(kvMaxCount, 4, 8) {
  val defaultValue = -1.0

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

  def put(key: Int, valueList: ArrayList[Integer]): Unit = {
    var pos = key & mask
    var step = 1
    while (step < maxProbes) {
      if (pointers(pos) == elementNotExist) {
        //System.out.println("===============insert key "+key+" pos is"+pos+ "===================")
        insert(pos, key, valueList)
        return
      } else {
        val pointer = pointers(pos)
        if (UNSAFE.getInt(pointer) == key) {
          val vlLength = if (valueList == null) {
            0
          } else {
            valueList.size()
          }
          val vlAddress = UNSAFE.allocateMemory((vlLength + 1) * 4 + 8)
          UNSAFE.putDouble(vlAddress, -1.0)
          UNSAFE.putInt(vlAddress + 8, vlLength)
          var i = 0;
          while (i < vlLength) {
            UNSAFE.putInt(vlAddress + 12 + i * 4, valueList.get(i))
            i += 1
          }

          UNSAFE.putLong(pointer + kSize, vlAddress)
          return
        }
      }
      pos = (pos + step) & mask
      step += 1
    }
    throw new UnsupportedOperationException
  }

  private def insert(pos: Int, key: Int, valueList: ArrayList[Integer]) {
    //    println(s"put at $pos")
    if (kvCount == kvMaxCount) {
      //println("kvcount is "+ kvCount+ "key is" + key + "pos is " + pos + "kvmaxCount is " + kvMaxCount)
      throw new UnsupportedOperationException
    }
    kvCount += 1
    pointers(pos) = curAddress

    UNSAFE.putInt(curAddress, key)
    curAddress += kSize
    //写堆外
    val vlLength = if (valueList == null) {
      0
    } else {
      valueList.size()
    }
    val vlAddress = UNSAFE.allocateMemory((vlLength + 1) * 4 + 8)
    //System.out.println("===============key "+key +"curAdress is "+ curAddress+" vlAddress is "+vlAddress+"================");
    UNSAFE.putDouble(vlAddress, -1.0)
    UNSAFE.putInt(vlAddress + 8, vlLength)
    var i = 0;
    while (i < vlLength) {
      UNSAFE.putInt(vlAddress + 12 + i * 4, valueList.get(i))
      i += 1
    }

    //end
    UNSAFE.putLong(curAddress, vlAddress)
    curAddress += vSize
  }

  def orderGetKey(index: Int): Int = {
    UNSAFE.getInt(address + 12 * index)
  }

  def orderGetValue(index: Int): Long = {
    UNSAFE.getLong(address + 12 * index + 4)
  }

  def putPairDouble(vladdress: Long, value: Double): Unit = {
    UNSAFE.putDouble(vladdress, value)
  }

  def getPairDouble(vlAddress: Long): Double = {
    UNSAFE.getDouble(vlAddress)
  }

  def getPairVLLength(vlAddress: Long): Int = {
    UNSAFE.getInt(vlAddress + 8)
  }

  def getPairVlValue(vlAddress: Long, index: Int): Int = {
    UNSAFE.getInt(vlAddress + 12 + index * 4)
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

