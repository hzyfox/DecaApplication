package microBenchmark;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.concurrent.*;

/**
 * Created by zx on 16-4-11.
 */
public class MultiThreadDecaJavaLR extends LR {

    private ExecutorService executor;
    private int partitions;
    private int cores;
    private double[] w;
    private Chunk[] cacheBytes;

    MultiThreadDecaJavaLR(int partitions, int cores) {
        this.partitions = partitions;
        this.cores = cores;
    }

    @Override
    public void textFile(int dimensions, int nums) {
        D = dimensions;
        this.N = nums;

        w = new double[D];
        for (int i = 0; i < D; i++) {
            w[i] = 2 * random.nextDouble() - 1;
        }

        try {
            int offset = 0;
            cacheBytes = new Chunk[partitions];
            for (int i = 0; i < partitions; i++) {
                cacheBytes[i] = new Chunk(D, N * (D + 1) / partitions);
            }

            int partitionId = 0;
            for (int i = 0; i < N; i++) {
                int y;
                if (i % 2 == 0) {
                    y = 0;
                } else {
                    y = 1;
                }

                if (partitionId >= partitions) {
                    partitionId -= partitions;
                    offset += (D + 1);
                }
                cacheBytes[partitionId].putValue(y, offset);
                offset += 1;
                for (int j = 0; j < D; j++) {
                    double tmp = random.nextGaussian() + y * R;
                    cacheBytes[partitionId].putValue(tmp, offset);
                    offset += 1;
                }
                partitionId += 1;
                offset -= (D + 1) ;
            }
        } catch (Exception e) {
            System.out.println("textFile error: " + e);
        }
    }

    @Override
    public void compute(int iterations) {
        executor = Executors.newFixedThreadPool(cores);
        for (int iter = 0; iter < iterations; iter++) {
            Future[] futures = new Future[partitions];
            try {
                for (int i = 0; i < partitions; i++) {
                    Callable callable = new RunThread(i);
                    futures[i] = executor.submit(callable);
                }
                double[] gradient = new double[D];
                for (int i = 0; i < partitions; i++) {
                    double[] subgradient = (double[]) futures[i].get(100, TimeUnit.MINUTES);
                    for (int j = 0; j < D; j++) {
                        gradient[j] += subgradient[j];
                    }
                }
                for (int j = 0; j < D; j++) {
                    w[j] -= gradient[j];
                }
            } catch (Exception e) {
                System.out.println("compute error: " + e);
            }
        }
//        System.out.print("Final w: ");
//        System.out.println(Arrays.toString(w));
    }

    @Override
    public void shutdown() {
        executor.shutdown();
    }

    class Chunk {
        int dimensions;
        int size;
        Unsafe unsafe;
        long[] points;
        long longArrayOffset;
        long longArrayScale;
        long count = 0;

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
            count += 1;
        }
        final double getFromPoints(long offset){
            return Double.longBitsToDouble(unsafe.getLong(points,longArrayOffset+longArrayScale*offset));
        }
        double[] getValue() {
            double[] gradient = new double[dimensions];
            long offset = 0;
            double y;
            while (offset < count) {
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

    class RunThread implements Callable {
        int partitionId;

        RunThread(int partitionId) {
            this.partitionId = partitionId;
        }

        @Override
        public Object call() {
            return cacheBytes[partitionId].getValue();
        }

    }
}
