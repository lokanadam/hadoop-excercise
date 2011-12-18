import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class DataPrepare {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
		private Text pair = new Text();
		private IntWritable movieId = new IntWritable();

		public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			String line = value.toString();
			String items[] = line.split(",");

			if (items.length == 3){
				FileSplit fileSplit = (FileSplit)reporter.getInputSplit();
				String fileName = fileSplit.getPath().getName();
				movieId.set(Integer.parseInt(fileName.substring(fileName.indexOf('_')+1,fileName.indexOf('.'))));
				pair.set(items[0]+":"+items[1]);	
				output.collect(movieId, pair);
			}	
		}

	}

	public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
		private Text vector = new Text();
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			String tmp = "";
			while(values.hasNext()){
				tmp += values.next().toString();
				if ( values.hasNext() )
					tmp +=",";
			}
			vector.set(tmp);
			output.collect(key, vector);	
		}
	}
	public static void runJob(String input, String output) throws Exception {

		JobConf conf = new JobConf(DataPrepare.class);
		conf.setJobName("data prepare");

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		FileSystem.get(conf).delete(new Path(output), true);

		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));

		JobClient.runJob(conf);
	}
	public static void main(String args[]) throws Exception {
		runJob(args[0], args[1]);
	}
}
