import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Viewer {
	public static class Map extends MapReduceBase implements Mapper<IntWritable, Text, IntWritable, Text> {
		private Text pair = new Text();
		private IntWritable movieId = new IntWritable();

		public void map(IntWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
		
			output.collect(key, new Text(value.toString().split(";")[1]));
		}

	}

	public static void runJob(String input, String output) throws Exception {

		JobConf conf = new JobConf(DataPrepare.class);
		conf.setJobName("Viewer");

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileSystem.get(conf).delete(new Path(output), true);

		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));

		JobClient.runJob(conf);
	}
	public static void main(String args[]) throws Exception {
		runJob(args[0], args[1]);
	}
}
