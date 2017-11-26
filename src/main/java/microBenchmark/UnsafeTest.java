package microBenchmark;

import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * create with microBenchmark
 * USER: husterfox
 */
public class UnsafeTest {

    public static void main(String[] args) throws NoSuchFieldException, IllegalAccessException {
        Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
        unsafeField.setAccessible(true);
        Unsafe unsafe = (Unsafe) unsafeField.get(null);

        Double[] testDoubleArray = new Double[]{1.1234d, 2.3456d, 3.4556d};
        long doubleArrayOffset = unsafe.arrayBaseOffset(Double[].class);
        long doubleArrayScale = unsafe.arrayIndexScale(Double[].class);
        System.out.println(" doubleArrayOffset is： " + doubleArrayOffset + "doubleArrayScale is: " + doubleArrayScale);
        System.out.println("get value array index 0: " + unsafe.getObjectVolatile(testDoubleArray,
                doubleArrayOffset));
        System.out.println("get value array index 1: " + unsafe.getObjectVolatile(testDoubleArray,
                doubleArrayOffset + doubleArrayScale));
        System.out.println("get value array index 2: " + unsafe.getObjectVolatile(testDoubleArray,
                doubleArrayOffset + doubleArrayScale * 2));


        long[] testLongArray = new long[]{123L, 345L, 456L};
        long longArrayOffset = unsafe.arrayBaseOffset(long[].class);
        long longArrayScale = unsafe.arrayIndexScale(long[].class);
        System.out.println(" longArrayOffset is： " + longArrayOffset + "longArrayScale is: " + longArrayOffset);
        System.out.println("get value array index 0: " + unsafe.getLongVolatile(testLongArray,
                longArrayOffset));
        System.out.println("get value array index 1: " + unsafe.getLongVolatile(testLongArray,
                longArrayOffset + longArrayScale));
        System.out.println("get value array index 2: " + unsafe.getLongVolatile(testLongArray,
                longArrayOffset + longArrayScale * 2));

        unsafe.putLong(testDoubleArray, longArrayOffset + longArrayScale, 357L);
        System.out.println("get value array index 1: " + unsafe.getLongVolatile(testLongArray,
                longArrayOffset + longArrayScale));

        long doubleIndex0 = Double.doubleToLongBits((Double) unsafe.getObjectVolatile(testDoubleArray, doubleArrayOffset));
        long doubleIndex1 = Double.doubleToLongBits((Double) unsafe.getObjectVolatile(testDoubleArray, doubleArrayOffset + doubleArrayScale));
        long result = doubleIndex0 * doubleIndex1;
        System.out.println("long value is " + doubleIndex0);
        System.out.println("long value is " + doubleIndex1);
        System.out.println("long result is "+ result);
        System.out.println("after exchange is "+Double.longBitsToDouble(result));


    }


}
