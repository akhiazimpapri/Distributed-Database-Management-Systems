import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class WordCount {

    // ============================================================
    // MAPPER
    // ============================================================

    public static class Map extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value,
                        OutputCollector<Text, IntWritable> output,
                        Reporter reporter) throws IOException {

            String line = value.toString();

            // Choose ONLY ONE function

        //     wordFrequency(line, output);
        //     characterFrequency(line, output);
            wordLengthFrequency(line, output);
            // lineLengthCount(line, output);
            // wordsPerLine(line, key, output);
            // charactersPerLine(line, key, output);
            // lineCharacterLengthCount(line, output);
        }
    }


    // ============================================================
    // 1. WORD FREQUENCY
    // ============================================================

    public static void wordFrequency(
            String line,
            OutputCollector<Text, IntWritable> output)
            throws IOException {

        StringTokenizer tokenizer =
                new StringTokenizer(line);

        while (tokenizer.hasMoreTokens()) {

            String word = tokenizer.nextToken();

            Text key = new Text(word);
            IntWritable value = new IntWritable(1);

            // Mapper output
            System.out.println(
                    "MAPPER OUTPUT: " + word + " -> 1"
            );

            output.collect(key, value);
        }
    }


    // ============================================================
    // 2. CHARACTER FREQUENCY
    // ============================================================

    public static void characterFrequency(
            String line,
            OutputCollector<Text, IntWritable> output)
            throws IOException {

        for (int i = 0; i < line.length(); i++) {

            char ch = line.charAt(i);

            // Ignore spaces
            if (ch != ' ') {

                Text key =
                        new Text(String.valueOf(ch));

                IntWritable value =
                        new IntWritable(1);

                // Mapper output
                System.out.println(
                        "MAPPER OUTPUT: " + ch + " -> 1"
                );

                output.collect(key, value);
            }
        }
    }

    


    // ============================================================
    // 3. WORD LENGTH FREQUENCY
    // ============================================================

    public static void wordLengthFrequency(
            String line,
            OutputCollector<Text, IntWritable> output)
            throws IOException {

        StringTokenizer tokenizer =
                new StringTokenizer(line);

        while (tokenizer.hasMoreTokens()) {

            String word = tokenizer.nextToken();

            int length = word.length();

            Text key =
                    new Text(String.valueOf(length));

            IntWritable value =
                    new IntWritable(1);

            // Mapper output
            System.out.println(
                    "MAPPER OUTPUT: " +
                    length + " -> 1"
            );

            output.collect(key, value);
        }
    }


    // ============================================================
    // 4. LINE LENGTH COUNT
    // Number of lines having the same number of words
    // ============================================================

    public static void lineLengthCount(
            String line,
            OutputCollector<Text, IntWritable> output)
            throws IOException {

        StringTokenizer tokenizer =
                new StringTokenizer(line);

        int wordCount = 0;

        while (tokenizer.hasMoreTokens()) {

            tokenizer.nextToken();
            wordCount++;
        }

        Text key =
                new Text(String.valueOf(wordCount));

        IntWritable value =
                new IntWritable(1);

        // Mapper output
        System.out.println(
                "MAPPER OUTPUT: " +
                wordCount + " -> 1"
        );

        output.collect(key, value);
    }


    // ============================================================
    // 5. NUMBER OF WORDS PER LINE
    // ============================================================

    public static void wordsPerLine(
            String line,
            LongWritable key,
            OutputCollector<Text, IntWritable> output)
            throws IOException {

        StringTokenizer tokenizer =
                new StringTokenizer(line);

        int wordCount = 0;

        while (tokenizer.hasMoreTokens()) {

            tokenizer.nextToken();
            wordCount++;
        }

        Text lineKey =
                new Text("Line_" + key.get());

        IntWritable value =
                new IntWritable(wordCount);

        // Mapper output
        System.out.println(
                "MAPPER OUTPUT: " +
                lineKey + " -> " + wordCount
        );

        output.collect(lineKey, value);
    }


    // ============================================================
    // 6. NUMBER OF CHARACTERS PER LINE
    // ============================================================

    public static void charactersPerLine(
            String line,
            LongWritable key,
            OutputCollector<Text, IntWritable> output)
            throws IOException {

        int characterCount = line.length();

        Text lineKey =
                new Text("Line_" + key.get());

        IntWritable value =
                new IntWritable(characterCount);

        // Mapper output
        System.out.println(
                "MAPPER OUTPUT: " +
                lineKey + " -> " +
                characterCount
        );

        output.collect(lineKey, value);
    }


    // ============================================================
    // 7. NUMBER OF LINES HAVING THE SAME NUMBER OF CHARACTERS
    // ============================================================

    public static void lineCharacterLengthCount(
            String line,
            OutputCollector<Text, IntWritable> output)
            throws IOException {

        int characterCount = line.length();

        Text key =
                new Text(String.valueOf(characterCount));

        IntWritable value =
                new IntWritable(1);

        // Mapper output
        System.out.println(
                "MAPPER OUTPUT: " +
                characterCount + " -> 1"
        );

        output.collect(key, value);
    }


    // ============================================================
    // REDUCER
    // ============================================================

    public static class Reduce extends MapReduceBase
            implements Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(
                Text key,
                Iterator<IntWritable> values,
                OutputCollector<Text, IntWritable> output,
                Reporter reporter)
                throws IOException {

            /*
             * For problems 1-4:
             * Sum all values.
             *
             * For problems 5-6:
             * There is normally only one value for each line key,
             * so simply return that value.
             *
             * The generic sum also works because there is only
             * one value for each line key.
             */

            int sum = 0;

            while (values.hasNext()) {

                sum += values.next().get();
            }

            // Reducer output
            System.out.println(
                    "REDUCER OUTPUT: " +
                    key.toString() +
                    " -> " +
                    sum
            );

            output.collect(
                    key,
                    new IntWritable(sum)
            );
        }
    }


    // ============================================================
    // MAIN
    // ============================================================

    public static void main(String[] args)
            throws Exception {

        JobConf conf =
                new JobConf(WordCount.class);

        conf.setJobName("MapReduce Task");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);

        conf.setCombinerClass(Reduce.class);

        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(
                TextInputFormat.class
        );

        conf.setOutputFormat(
                TextOutputFormat.class
        );

        FileInputFormat.setInputPaths(
                conf,
                new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
                conf,
                new Path(args[1])
        );

        JobClient.runJob(conf);
    }
}