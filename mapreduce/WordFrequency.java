import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class WordFrequency {

    public static class MapperClass
            extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value,
                        OutputCollector<Text, IntWritable> output,
                        Reporter reporter) throws IOException {

            String[] words = value.toString().split("\\s+");

            for (String word : words) {
                output.collect(
                    new Text(word),
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

        JobConf conf = new JobConf(WordFrequency.class);

        conf.setMapperClass(MapperClass.class);
        conf.setReducerClass(ReducerClass.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}