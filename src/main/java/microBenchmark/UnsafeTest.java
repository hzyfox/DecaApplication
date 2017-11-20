package microBenchmark;

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

        double[] testDoubleArray = new double[]{1.1234d, 2.3456d, 3.4556d};
        long doubleArrayOffset = unsafe.arrayBaseOffset(double[].class);
        long doubleArrayScale = unsafe.arrayIndexScale(double[].class);
        System.out.println(" doubleArrayOffset is： " + doubleArrayOffset + "doubleArrayScale is: " + doubleArrayScale);
        System.out.println("get value array index 0: " + unsafe.getDoubleVolatile(testDoubleArray,
                doubleArrayOffset));
        System.out.println("get value array index 1: " + unsafe.getDoubleVolatile(testDoubleArray,
                doubleArrayOffset + doubleArrayScale));
        System.out.println("get value array index 2: " + unsafe.getDoubleVolatile(testDoubleArray,
                doubleArrayOffset + doubleArrayScale*2));



        //Field doubleValueField = Double.class.getDeclaredField("value");
        //long valueOffset = unsafe.objectFieldOffset(doubleValueField);
        //doubleValueField.setAccessible(true);
        //Double testDouble = 20.123456d;
       //System.out.println("value filed offset is: " + valueOffset);
        //System.out.println("get Double Value"+unsafe.getDouble(testDouble,valueOffset));
        //System.out.println("get Double Value"+unsafe.getObjectVolatile(testDouble,valueOffset));这句话会造出内存错误

    }


}
