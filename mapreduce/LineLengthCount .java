import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class LineLengthCount {

    public static class MapperClass
            extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value,
                        OutputCollector<Text, IntWritable> output,
                        Reporter reporter) throws IOException {

            String line = value.toString();

            int wordCount = line.split("\\s+").length;

            output.collect(
                new Text(String.valueOf(wordCount)),
                new IntWritable(1)
            );
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

        JobConf conf = new JobConf(LineLengthCount.class);

        conf.setMapperClass(MapperClass.class);
        conf.setReducerClass(ReducerClass.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
