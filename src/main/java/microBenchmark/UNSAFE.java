package microBenchmark;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * create with microBenchmark
 * USER: husterfox
 */
public class UNSAFE {
    static HashMap<Integer, long[]> unsafeLongArray = new HashMap<Integer, long[]>(200);
    /**
     * use ArrayList can not support remove element，or will cause the index change.
     */
    static ArrayList<long[]> unsafeLongArrayList = new ArrayList<long[]>(200);
    static Field unsafeField = null;
    static Unsafe unsafe = null;
    static int longArrayIndex = 0;
    static int INTLENGTH = 32;
    /**
     * kind will be Assigned in PRHelper
     * 0 use sun Unsafe ||1 use HashMap self Unsafe || 2 use ArrayList self Unsafe
     */
    static int kind;
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
        if (kind == 0) {
            throw new RuntimeException("This is only for test in JVM sun Unsafe, uncomment the next comment");
            //return unsafe.allocateMemory(size);
        } else {
            if (kind == 1) {
                long[] allocateLong = new long[(int) (size / 8 + 1)];
                unsafeLongArray.put(longArrayIndex, allocateLong);
                long address = (long) longArrayIndex << INTLENGTH | longArrayOffset;
                longArrayIndex += 1;
                //because index is not change,so index should be in higher bit
                //不转型为long 左移32位将永远为0
                return address;
            } else {
                if (kind == 2) {
                    long[] allocateLong = new long[(int) (size / 8 + 1)];
                    unsafeLongArrayList.add(allocateLong);
                    int index = unsafeLongArrayList.indexOf(allocateLong);
                    return (long) index << INTLENGTH | longArrayOffset;
                } else {
                    throw new RuntimeException("unsupported kind " + kind);
                }
            }
        }
    }


    synchronized static void freeMemory(long address) {
        if (kind == 0) {
            throw new RuntimeException("This is only for test in JVM sun Unsafe, uncomment the next comment");
            //unsafe.freeMemory(address);
        } else {
            if (kind == 1) {
                int index = getLongArrayIndex(address);
                unsafeLongArray.remove(index);
            } else {
                if (kind == 2) {
                    int index = getLongArrayIndex(address);
                    unsafeLongArrayList.add(index, new long[]{-1});
                    System.out.printf("remove length is %d\n",unsafeLongArrayList.remove(index + 1).length);
                } else {
                    throw new RuntimeException("unsupported kind " + kind);
                }
            }
        }
    }

    static void putInt(long address, int value) {
        if (kind == 0) {
            throw new RuntimeException("This is only for test in JVM sun Unsafe, uncomment the next comment");
            //unsafe.putInt(address, value);
        } else {
            if (kind == 1 || kind == 2) {
                int index = getLongArrayIndex(address);
                address = getLongArrayAddress(address);
                long[] targetLongArray = getTargetLongArray(index);
                unsafe.putInt(targetLongArray, address, value);
            } else {
                throw new RuntimeException("unsupported kind " + kind);
            }
        }

    }

    static int getInt(long address) {
        if (kind == 0) {
            throw new RuntimeException("This is only for test in JVM sun Unsafe, uncomment the next comment");
            //return unsafe.getInt(address);
        } else {
            if (kind == 1 || kind == 2) {
                int index = getLongArrayIndex(address);
                address = getLongArrayAddress(address);
                long[] targetLongArray = getTargetLongArray(index);
                return unsafe.getInt(targetLongArray, address);
            } else {
                throw new RuntimeException("unsupported kind " + kind);
            }
        }

    }

    static double getDouble(long address) {
        if (kind == 0) {
            throw new RuntimeException("This is only for test in JVM sun Unsafe, uncomment the next comment");
            //return unsafe.getDouble(address);
        } else {
            if (kind == 1 || kind == 2) {
                int index = getLongArrayIndex(address);
                address = getLongArrayAddress(address);
                long[] targetLongArray = getTargetLongArray(index);
                return Double.longBitsToDouble(unsafe.getLong(targetLongArray, address));
            } else {
                throw new RuntimeException("unsupported kind " + kind);
            }
        }

    }

    static void putDouble(long address, double value) {
        if (kind == 0) {
            throw new RuntimeException("This is only for test in JVM sun Unsafe, uncomment the next comment");
            //unsafe.putDouble(address, value);
        } else {
            if (kind == 1 || kind == 2) {
                int index = getLongArrayIndex(address);
                address = getLongArrayAddress(address);
                long[] targetLongArray = getTargetLongArray(index);
                unsafe.putLong(targetLongArray, address, Double.doubleToLongBits(value));
            } else {
                throw new RuntimeException("unsupported kind " + kind);
            }
        }

    }

    static long getLong(long address) {
        if (kind == 0) {
            throw new RuntimeException("This is only for test in JVM sun Unsafe, uncomment the next comment");
            //return unsafe.getLong(address);
        } else {
            if (kind == 1 || kind == 2) {
                int index = getLongArrayIndex(address);
                address = getLongArrayAddress(address);
                long[] targetLongArray = getTargetLongArray(index);
                return unsafe.getLong(targetLongArray, address);
            } else {
                throw new RuntimeException("unsupported kind " + kind);
            }
        }

    }

    static void putLong(long address, long value) {
        if (kind == 0) {
            throw new RuntimeException("This is only for test in JVM sun Unsafe, uncomment the next comment");
            //unsafe.putLong(address, value);
        } else {
            if (kind == 1 || kind == 2) {
                int index = getLongArrayIndex(address);
                address = getLongArrayAddress(address);
                long[] targetLongArray = getTargetLongArray(index);
                unsafe.putLong(targetLongArray, address, value);
            } else {
                throw new RuntimeException("unsupported kind " + kind);
            }
        }

    }

    static int getLongArrayIndex(long address) {
        return (int) (address >> 32);
    }

    static long getLongArrayAddress(long address) {
        return address & 0xffffffffL;
    }

    static long[] getTargetLongArray(int index) {
        if (kind == 1) {
            return unsafeLongArray.get(index);
        } else {
            if (kind == 2) {
                return unsafeLongArrayList.get(index);
            } else {
                throw new RuntimeException("do not support kind " + kind +
                        " 0 use sun Unsafe ||1 use HashMap self Unsafe || 2 use ArrayList self Unsafe");
            }
        }
    }

}
