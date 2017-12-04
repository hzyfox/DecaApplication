package microBenchmark;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public class MultiThreadDecaPR extends MultiThreadPR {

    public IntLongMap[] blocks;

    public MultiThreadDecaPR(int numCores, int numPartitions) {
        super(numCores, numPartitions);
        name = "MultiThreadDecaPR";
    }

    @Override
    protected void cache(Map<Integer, ArrayList<Integer>> links) {
        super.cache(links);

        ////////////////////
        blocks = new IntLongMap[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            //System.out.println("--------------idcount is " + idCount + "-----------------");
            blocks[i] = new IntLongMap(idCount);
        }
        for (Map.Entry<Integer, ArrayList<Integer>> entry : links.entrySet()) {
            //System.out.println("cache phase:-------------" + "put partition " + entry.getKey() % numPartitions +"put key " + entry.getKey()+ "-------------------");
            blocks[entry.getKey() % numPartitions].put(entry.getKey(), entry.getValue());
        }
        if(blocks[0].get(0)==-1){
            //System.out.println("cache phase:--------------- get key 0 failure --------------------");
        }
        for (int i = 0; i < numPartitions; i++) {
            //System.out.println("cache phase: partition " + i + "----------after cache put , kvcount is " + blocks[i].kvCount() + "-----------------------");
        }
    }

    @Override
    public void compute(int iterations) {
        IntDoubleMap[][] outMessages = new IntDoubleMap[numPartitions][];
        Future<IntDoubleMap[]>[] futures = new Future[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            futures[i] = executor.submit(new InitTask(i));
        }
        for (int i = 0; i < numPartitions; i++) {
            try {
                outMessages[i] = futures[i].get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        IntDoubleMap[][] inMessages = new IntDoubleMap[numPartitions][numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            for (int j = 0; j < numPartitions; j++) {
                inMessages[j][i] = outMessages[i][j];
            }
        }
        for (int i = 0; i < numPartitions; i++) {
            //System.out.println("partition " + i + "----------after init task , kvcount is " + blocks[i].kvCount() + "-----------------------");
        }
        for (int iter = 1; iter < iterations; iter++) {
            for (int i = 0; i < numPartitions; i++) {
                futures[i] = executor.submit(new IterTask(i, inMessages[i]));
            }
            for (int i = 0; i < numPartitions; i++) {
                try {
                    outMessages[i] = futures[i].get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            for (int i = 0; i < numPartitions; i++) {
                for (int j = 0; j < numPartitions; j++) {
                    inMessages[j][i] = outMessages[i][j];
                }
            }
        }

        Future<IntDoubleMap>[] resultFutures = new Future[numPartitions];
        IntDoubleMap[] results = new IntDoubleMap[numPartitions];

        for (int i = 0; i < numPartitions; i++) {
            resultFutures[i] = executor.submit(new EndTask(i, inMessages[i]));
        }
        for (int i = 0; i < numPartitions; i++) {
            try {
                results[i] = resultFutures[i].get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        for (int i = 0; i < numPartitions; i++) {
            System.out.println(results[i].toString());
        }
    }

    private class InitTask implements Callable<IntDoubleMap[]> {
        int partitionId;

        InitTask(int partitionId) {
            this.partitionId = partitionId;
        }

        @Override
        public IntDoubleMap[] call() throws Exception {
            IntLongMap block = blocks[partitionId];
            int[] counts = mapOutKeyCounts[partitionId];
            IntDoubleMap[] result = new IntDoubleMap[numPartitions];
            for (int i = 0; i < numPartitions; i++) {
                result[i] = new IntDoubleMap(counts[i]);
            }
            for (int i = 0; i < block.kvCount(); i++) {
                int key = block.orderGetKey(i);
                long vlAddress = block.orderGetValue(i);
                int vlLength = block.getPairVLLength(vlAddress);
                final double value = 1.0 / vlLength;
                for (int j = 0; j < vlLength; j++) {
                    int url = block.getPairVlValue(vlAddress, j);
                    double newValue = value;
                    IntDoubleMap outMap = result[url % numPartitions];
                    if (outMap.get(url) != -1.0) {
                        newValue += outMap.get(url);
                    }
                    outMap.put(url, newValue);

                }

            }


            return result;

        }
    }

    private class IterTask implements Callable<IntDoubleMap[]> {

        int partitionId;
        IntDoubleMap[] inMessages;

        IterTask(int partitionId, IntDoubleMap[] inMessages) {
            this.partitionId = partitionId;
            this.inMessages = inMessages;
        }

        @Override
        public IntDoubleMap[] call() throws Exception {
            IntLongMap block = blocks[partitionId];
            int count = reduceInKeyCounts[partitionId];

            IntDoubleMap reduceMap = new IntDoubleMap(count);

            for (IntDoubleMap inMessage : inMessages) {
                for (int i = 0; i < inMessage.kvCount(); i++) {
                    int k = (int) inMessage.orderGetKey(i);
                    double v = (double) inMessage.orderGetValue(i);
                    if (reduceMap.get(k) != -1.0) {
                        v += reduceMap.get(k);
                    }
                    reduceMap.put(k, v);
                }
            }
            for (int i = 0; i < reduceMap.kvCount(); i++) {
                int key = reduceMap.orderGetKey(i);
                double value = reduceMap.orderGetValue(i);
                if(block.get(0) ==-1){
                    //System.out.println("iter phase: ------------" +"get key 0 failure ---------------------");
                }
                if (block.get(key) != -1) {
                    //System.out.println("iter phase:-------------"+"put partition "+partitionId+" key " +key+"-------------------");
                    block.putPairDouble(block.get(key), value * 0.85 + 0.15);
                } else {
                    //System.out.println("iter phase partition " + partitionId + "---------inter task------ put key " + key);
                    block.put(key, null); //2 边出现在右边 不在左边 block里面没有d
                }
            }
            int[] counts = mapOutKeyCounts[partitionId];
            IntDoubleMap[] result = new IntDoubleMap[numPartitions];
            for (int i = 0; i < numPartitions; i++) {
                result[i] = new IntDoubleMap(counts[i]);
            }

            for (int i = 0; i < block.kvCount(); i++) {
                int key = block.orderGetKey(i);
                long vlAddress = block.orderGetValue(i);

                if (block.getPairDouble(vlAddress) != -1.0 &&
                        block.getPairVLLength(vlAddress) != 0) {
                    double dv = block.getPairDouble(vlAddress);
                    int vlLength = block.getPairVLLength(vlAddress);
                    final double value = dv / vlLength;
                    for (int j = 0; j < vlLength; j++) {
                        int url = block.getPairVlValue(vlAddress, j);
                        double newValue = value;
                        IntDoubleMap outMap = result[url % numPartitions];
                        if (outMap.get(url) != -1.0) {
                            newValue += outMap.get(url);
                        }
                        outMap.put(url, newValue);
                    }
                }

            }
            return result;


        }
    }

    private class EndTask implements Callable<IntDoubleMap> {
        int partitionId;
        IntDoubleMap[] inMessages;

        EndTask(int partitionId, IntDoubleMap[] inMessages) {
            this.partitionId = partitionId;
            this.inMessages = inMessages;
        }

        @Override
        public IntDoubleMap call() throws Exception {
            IntDoubleMap reduceMap = new IntDoubleMap(idCount);
            for (IntDoubleMap inMessage : inMessages) {
                for (int i = 0; i < inMessage.kvCount(); i++) {
                    int k = (int) inMessage.orderGetKey(i);
                    double v = (double) inMessage.orderGetValue(i);
                    if (reduceMap.get(k) != -1.0) {
                        v += reduceMap.get(k);
                    }
                    reduceMap.put(k, v);
                }
            }
            return reduceMap;

        }
    }

}

