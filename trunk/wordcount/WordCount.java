import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class WordCount {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(scrub(tokenizer.nextToken()));
				output.collect(word, one);
			}
		}

		private String scrub(String noisyword) {return noisyword.toLowerCase().replaceAll("\\pP|\\pS", "");}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public static class SortMapper extends MapReduceBase implements Mapper<Text, IntWritable, IntWritable, Text> {
		public void map(Text key, IntWritable value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			output.collect(value, key);
		}
	}

	public static class InvertedComparator extends IntWritable.Comparator {
		public int compare(WritableComparable a, WritableComparable b) { return -super.compare(a, b); }
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) { return -super.compare(b1, s1, l1, b2, s2, l2); }
	}

	public static void main(String args[]) throws Exception {
		String tmpPath = "/wordcount/tmp";

		// count job
		JobConf conf = new JobConf(WordCount.class);
		conf.setJobName("wordcount");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		FileSystem.get(conf).delete(new Path(tmpPath), true);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(tmpPath));

		JobClient.runJob(conf);

		// sort job
		conf = new JobConf(WordCount.class);
		conf.setJobName("sortjob");

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputKeyComparatorClass(InvertedComparator.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(SortMapper.class);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileSystem.get(conf).delete(new Path(args[1]), true);

		FileInputFormat.setInputPaths(conf, new Path(tmpPath));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}
