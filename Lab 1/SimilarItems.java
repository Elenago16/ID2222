package id2222;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.*;

public class SimilarItems {

    final static private int SHINGLE_LENGTH = 6;
    final static private int SIGNATURE_LENGTH = 100; // the length of the signature
    final static private double SIMILARITY_THRESHOLD = 0.8;
    final static private int PRIME = 813768601; // prime number

    static int coef[][] = new int[SIGNATURE_LENGTH][2];

    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        Random rand = new Random();

        String[] filenames = {"alice","doriangrey","leviathan","mobydick","pygmalion","test1","test2", "warandpeace"};
        int size = filenames.length;
        ArrayList<DataSet> shingleSets = new ArrayList<>(size);
        ArrayList<DataSet> signatures = new ArrayList<>(size);
        ArrayList<DataSet> lshs = new ArrayList<>(size);
        StringBuilder sb = new StringBuilder("\n");

        for (int i = 0; i < SIGNATURE_LENGTH; i++) {
            coef[i][0] = rand.nextInt(PRIME);
            coef[i][1] = rand.nextInt(PRIME);
        }

        // loading up shingles, signatures and LSH for each file
        for (int i = 0; i < size; i++) {
            DataSet<String> file = env.readTextFile("src/documents/" + filenames[i] + ".txt");
            DataSet<Integer> shingle = file.flatMap(new Shingler()).distinct();
            shingleSets.add(i, shingle);
            DataSet<Tuple2<Integer, Integer>> signature = shingle.flatMap(new Hasher())
                    .groupBy(0).min(1);
            signatures.add(i, signature);
            lshs.add(signature.mapPartition(new LSH()).setParallelism(2));
        }

        // finding Jaccard Similarity for each pair of shingle sets

        long startTime = System.nanoTime();

        for (int i = 0; i < size; i++) {
            for (int j = i + 1; j < size; j++ ) {
                Long intersection = shingleSets.get(i).join(shingleSets.get(j)).where("*")
                        .equalTo("*").distinct().count();
                Long union = shingleSets.get(i).union(shingleSets.get(j)).distinct().count();
                double sim = (double) intersection / union;

                if (sim >= SIMILARITY_THRESHOLD) {
                    sb.append("Files " + filenames[i] + ".txt and " + filenames[j]);
                    sb.append(" have Jaccard Similarity " + sim + "\n");
                }
            }
        }

        long endTime = (System.nanoTime() - startTime) / 1000000000 ; // in sec

        sb.append("\nCalculation of text similarity for shigle sets is done.\n");
        sb.append("It took " + endTime + " seconds\n\n");

        // finding Jaccard Similarity for each pair of signatures

       startTime = System.nanoTime();

        for (int i = 0; i < size; i++) {
            for (int j = i + 1; j < size; j++ ) {

                Long intersection = signatures.get(i).join(signatures.get(j)).where("*")
                        .equalTo("*").distinct().count();
                double sim = (double) intersection / SIGNATURE_LENGTH;

                if (sim >= SIMILARITY_THRESHOLD) {
                    sb.append("Files " + filenames[i] + ".txt and " + filenames[j])
                            .append(" have Jaccard Similarity " + sim + "\n");
                }
            }
        }
        endTime = (System.nanoTime() - startTime) / 1000000000 ;
        sb.append("\nCalculation of text similarity for signatures is done.\n");
        sb.append("It took " + endTime + " seconds\n\n");

        // LSH

        startTime = System.nanoTime();

        for (int i = 0; i < size; i++) {
            for (int j = i + 1; j < size; j++ ) {

                ArrayList l1 = (ArrayList) lshs.get(i).collect();
                ArrayList l2 = (ArrayList) lshs.get(j).collect();

                boolean sim = false;

                for (Object value : l1) {
                    if (value.equals(l2.get(l1.indexOf(value)))) sim = true;
                }

                if (sim) sb.append("Files " + filenames[i] + ".txt and " + filenames[j])
                            .append(" are a candidate pair\n");
            }
        }

        endTime = (System.nanoTime() - startTime) / 1000000000 ;
        sb.append("\nLSH computation took " + endTime + " seconds\n\n");

        System.out.println(sb.toString());
    }


    static class Shingler implements FlatMapFunction<String, Integer> {

        public void flatMap(String value, Collector<Integer> out) {
            String s = value.toLowerCase().replaceAll("[^A-Za-z]+", "");

            if (s.length() <= SHINGLE_LENGTH) {
                out.collect(s.hashCode());
                return;
            }

            int noOfShingles = s.length() - SHINGLE_LENGTH + 1;

            for (int i = 0; i < noOfShingles; i++) {
                out.collect(s.substring(i, i + SHINGLE_LENGTH).hashCode());
            }
        }
    }

    static class Hasher implements FlatMapFunction<Integer, Tuple2<Integer, Integer>> {

        public void flatMap(Integer value, Collector<Tuple2<Integer, Integer>> out) {
            for (Integer i = 0; i < SIGNATURE_LENGTH; i++) {
                Integer currentminHash = (coef[i][0] * value + coef[i][1]) % PRIME;
                out.collect(new Tuple2<>(i, currentminHash));
            }
        }

    }

    static class LSH implements MapPartitionFunction<Tuple2<Integer, Integer>, Integer> {

        public void mapPartition(Iterable<Tuple2<Integer, Integer>> values, Collector<Integer> out) {

            int buckets = 20;
            int p = 140;

            int sum = 0;


            for (Tuple2<Integer, Integer> value : values) {
                sum = (7 * value.f1 + 13) % p;
            }

            int h = (sum & 0x7fffffff) % p;

            int bucketIndex = p / buckets;

            for (int i = 1; i <= buckets; i++) {
                if (h >= bucketIndex * (i - 1) && h < (bucketIndex * i)) out.collect(i);
            }

        }
    }
}
