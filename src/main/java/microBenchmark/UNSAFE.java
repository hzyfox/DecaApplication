package microBenchmark;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.HashMap;

/**
 * create with microBenchmark
 * USER: husterfox
 */
public class UNSAFE {
    static HashMap<Integer, long[]> unsafeLongArray;
    static Field unsafeField = null;
    static Unsafe unsafe = null;
    static int longArrayIndex = 0;
    static int INTLENGTH = 32;

    static long longArrayOffset;

    static {
        try {
            unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
        unsafeField.setAccessible(true);
        try {
            unsafe = (Unsafe) unsafeField.get(null);
            longArrayOffset = unsafe.arrayBaseOffset(long[].class);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    synchronized static long allocateMemory(long size) {
        long[] allocateLong = new long[(int) (size / 8 + 1)];
        unsafeLongArray.put(longArrayIndex, allocateLong);

        //because index is not change,so index should be in higher bit
        //不转型为long 左移32位将永远为0
        return (long) longArrayIndex++ << INTLENGTH | longArrayOffset;
    }


    static void freeMemory(long address) {
        int index = getLongArrayIndex(address);
        unsafeLongArray.remove(index);
    }

    static void putInt(long address, int value) {
        int index = getLongArrayIndex(address);
        address = getLongArrayAddress(address);
        long[] targetLongArray = getTargetLongArray(index);
        unsafe.putInt(targetLongArray, address, value);
    }

    static int getInt(long address) {
        int index = getLongArrayIndex(address);
        address = getLongArrayAddress(address);
        long[] targetLongArray = getTargetLongArray(index);
        return unsafe.getInt(targetLongArray, address);
    }

    static double getDouble(long address) {
        int index = getLongArrayIndex(address);
        address = getLongArrayAddress(address);
        long[] targetLongArray = getTargetLongArray(index);
        return Double.longBitsToDouble(unsafe.getLong(targetLongArray, address));
    }

    static void putDouble(long address, double value) {
        int index = getLongArrayIndex(address);
        address = getLongArrayAddress(address);
        long[] targetLongArray = getTargetLongArray(index);
        unsafe.putLong(targetLongArray, address, Double.doubleToLongBits(value));
    }

    static long getLong(long address) {
        int index = getLongArrayIndex(address);
        address = getLongArrayAddress(address);
        long[] targetLongArray = getTargetLongArray(index);
        return unsafe.getLong(targetLongArray, address);
    }

    static void putLong(long address, long value) {
        int index = getLongArrayIndex(address);
        address = getLongArrayAddress(address);
        long[] targetLongArray = getTargetLongArray(index);
        unsafe.putLong(targetLongArray, address, value);
    }

    static int getLongArrayIndex(long address) {
        return (int) (address >> 32);
    }

    static long getLongArrayAddress(long address) {
        return address & 0xffffffffL;
    }

    static long[] getTargetLongArray(int index) {
        return unsafeLongArray.get(index);
    }

}
