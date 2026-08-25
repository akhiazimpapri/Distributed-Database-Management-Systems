import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class WordLengthFrequency {

    public static class MapperClass
            extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value,
                        OutputCollector<Text, IntWritable> output,
                        Reporter reporter) throws IOException {

            String[] words = value.toString().split("\\s+");

            for (String word : words) {

                int length = word.length();

                output.collect(
                    new Text(String.valueOf(length)),
                    new IntWritable(1)
                );
            }
        }
    }

    public static class ReducerClass
            extends MapReduceBase
            implements Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterator<IntWritable> values,
                            OutputCollector<Text, IntWritable> output,
                            Reporter reporter) throws IOException {

            int sum = 0;

            while (values.hasNext())
                sum += values.next().get();

            output.collect(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        JobConf conf = new JobConf(WordLengthFrequency.class);

        conf.setMapperClass(MapperClass.class);
        conf.setReducerClass(ReducerClass.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}