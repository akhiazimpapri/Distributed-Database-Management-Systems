import java.io.IOException;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class WordCount {

    // ============================================================
    // MAPPER
    // For each word, emit (word, 1)
    // ============================================================

    public static class Map extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one =
                new IntWritable(1);

        private Text wordKey = new Text();

        public void map(
                LongWritable key,
                Text value,
                OutputCollector<Text, IntWritable> output,
                Reporter reporter)
                throws IOException {

            String line = value.toString();

            StringTokenizer tokenizer =
                    new StringTokenizer(line);

            while (tokenizer.hasMoreTokens()) {

                String word = tokenizer.nextToken();

                // Remove punctuation
                word = word.replaceAll("[^A-Za-z0-9]", "");

                if (!word.isEmpty()) {

                    wordKey.set(word);

                    // Mapper output
                    System.out.println(
                            "MAPPER OUTPUT: " +
                            word + " -> 1"
                    );

                    output.collect(wordKey, one);
                }
            }
        }
    }


    // ============================================================
    // CLASS TO STORE WORD AND COUNT
    // ============================================================

    public static class WordCountPair
            implements Comparable<WordCountPair> {

        String word;
        int count;

        public WordCountPair(String word, int count) {
            this.word = word;
            this.count = count;
        }

        // Smaller count comes first
        public int compareTo(WordCountPair other) {

            if (this.count != other.count) {

                return Integer.compare(
                        this.count,
                        other.count
                );
            }

            return this.word.compareTo(other.word);
        }
    }


    // ============================================================
    // REDUCER
    // First aggregate count of every word.
    // Then keep only TOP 3 words.
    // ============================================================

    public static class Reduce extends MapReduceBase
            implements Reducer<Text, IntWritable,
                               Text, IntWritable> {

        // PriorityQueue stores only top 3 words
        private PriorityQueue<WordCountPair> top3 =
                new PriorityQueue<WordCountPair>(3);


        public void reduce(
                Text key,
                Iterator<IntWritable> values,
                OutputCollector<Text, IntWritable> output,
                Reporter reporter)
                throws IOException {

            // ----------------------------------------------------
            // Calculate total count of current word
            // ----------------------------------------------------

            int sum = 0;

            while (values.hasNext()) {

                sum += values.next().get();
            }


            // Show aggregate count
            System.out.println(
                    "REDUCER COUNT: " +
                    key.toString() +
                    " -> " +
                    sum
            );


            // ----------------------------------------------------
            // Create word-count pair
            // ----------------------------------------------------

            WordCountPair pair =
                    new WordCountPair(
                            key.toString(),
                            sum
                    );


            // ----------------------------------------------------
            // Add word to PriorityQueue
            // ----------------------------------------------------

            top3.offer(pair);


            // ----------------------------------------------------
            // If more than 3 words exist,
            // remove the smallest count
            // ----------------------------------------------------

            if (top3.size() > 3) {

                top3.poll();
            }
        }


        // ========================================================
        // CLEANUP
        // Called after all words have been processed
        // ========================================================

        protected void cleanup(
                OutputCollector<Text, IntWritable> output,
                Reporter reporter)
                throws IOException {


            // Convert PriorityQueue to array
            WordCountPair[] result =
                    top3.toArray(
                            new WordCountPair[0]
                    );


            // ----------------------------------------------------
            // Sort from highest count to lowest count
            // ----------------------------------------------------

            for (int i = 0; i < result.length; i++) {

                for (int j = i + 1;
                     j < result.length;
                     j++) {

                    if (result[j].count >
                        result[i].count) {

                        WordCountPair temp =
                                result[i];

                        result[i] =
                                result[j];

                        result[j] =
                                temp;
                    }
                }
            }


            // ----------------------------------------------------
            // Output final TOP 3
            // ----------------------------------------------------

            System.out.println(
                    "===== TOP 3 WORDS ====="
            );

            for (WordCountPair pair : result) {

                System.out.println(
                        "TOP WORD: " +
                        pair.word +
                        " -> " +
                        pair.count
                );

                output.collect(
                        new Text(pair.word),
                        new IntWritable(pair.count)
                );
            }
        }
    }


    // ============================================================
    // MAIN
    // ============================================================

    public static void main(String[] args)
            throws Exception {

        JobConf conf =
                new JobConf(WordCount.class);


        conf.setJobName(
                "Top 3 Most Frequent Words"
        );


        // Output key and value type
        conf.setOutputKeyClass(
                Text.class
        );

        conf.setOutputValueClass(
                IntWritable.class
        );


        // Mapper
        conf.setMapperClass(
                Map.class
        );


        // Reducer
        conf.setReducerClass(
                Reduce.class
        );


        // IMPORTANT:
        // Use only ONE reducer so that
        // global TOP 3 can be calculated correctly.

        conf.setNumReduceTasks(1);


        // Input format
        conf.setInputFormat(
                TextInputFormat.class
        );


        // Output format
        conf.setOutputFormat(
                TextOutputFormat.class
        );


        // Input path
        FileInputFormat.setInputPaths(
                conf,
                new Path(args[0])
        );


        // Output path
        FileOutputFormat.setOutputPath(
                conf,
                new Path(args[1])
        );


        // Run MapReduce job
        JobClient.runJob(conf);
    }
}
