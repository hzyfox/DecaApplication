package microBenchmark;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * Created by zx on 16-4-6.
 */

public class DecaJavaLR extends LR {

    private double[] w;
    private Chunk cacheBytes;

    @Override
    public void textFile(int dimensions, int nums) {
        D = dimensions;
        this.N = nums;

        w = new double[D];
        for (int i = 0; i < D; i++) {
            w[i] = 2 * i * 0.037 - 1;
        }


        try {
            int offset = 0;
            cacheBytes = new Chunk(D, N * (D + 1));
            for (int i = 0; i < N; i++) {
                int y;
                if (i % 2 == 0) {
                    y = 0;
                } else {
                    y = 1;
                }
                cacheBytes.putValue(y, offset);
                offset += 1;
                for (int j = 0; j < D; j++) {
                    double tmp = random.nextGaussian() + y * R;
                    cacheBytes.putValue(tmp, offset);
                    offset += 1;
                }
            }
        } catch (Exception e) {
            System.out.println("textFile error: " + e);
        }
    }

    @Override
    public void compute(int iterations) {
        for (int iter = 0; iter < iterations; iter++) {
            double[] gradient = cacheBytes.getValue();
            for (int j = 0; j < D; j++) {
                w[j] -= gradient[j];
            }
        }
//        System.out.print("Final w: ");
//        System.out.println(Arrays.toString(w));
    }

    class Chunk {
        int dimensions;
        int size;
        Unsafe unsafe;
        long[] points;
        long longArrayOffset;
        long longArrayScale;

        Chunk(int dimensions, int size) {
            this.size = size;
            this.dimensions = dimensions;
            try {
                Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
                unsafeField.setAccessible(true);
                unsafe = (Unsafe) unsafeField.get(null);
                longArrayOffset = unsafe.arrayBaseOffset(long[].class);
                longArrayScale = unsafe.arrayIndexScale(long[].class);
                points = new long[size];
            } catch (Exception e) {
                System.out.println("init error: " + e);
            }
        }


        void putValue(double value, long offset) {
            unsafe.putLong(points, longArrayOffset + longArrayScale * offset, Double.doubleToLongBits(value));
        }

        final double getFromPoints(long offset) {
            return Double.longBitsToDouble(unsafe.getLong(points, longArrayOffset + longArrayScale * offset));
        }

        double[] getValue() {
            double[] gradient = new double[dimensions];
            long offset = 0;
            double y;
            while (offset < points.length) {
                //double[] gradient1 = new double[D];
                y = getFromPoints(offset);
                offset += 1;
                long current = offset;
                double dot = 0.0;
                for (int j = 0; j < dimensions; j++) {
                    dot += w[j] * getFromPoints(current);
                    current += 1;
                }
                double tmp = (1 / (1 + Math.exp(-y * dot)) - 1) * y;
                for (int j = 0; j < dimensions; j++) {
                    gradient[j] += tmp * getFromPoints(offset);
                    offset += 1;
                }
            }
            return gradient;
        }
    }

}
