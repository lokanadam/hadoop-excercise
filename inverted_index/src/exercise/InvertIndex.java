package exercise;

import java.util.*;
import java.io.*;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.*;

public class InvertIndex {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		private Text location = new Text();
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			FileSplit fileSplit = (FileSplit)reporter.getInputSplit();
			String fileName = fileSplit.getPath().getName();
			location.set(fileName);
			while (tokenizer.hasMoreTokens()){
				word.set(tokenizer.nextToken());
				output.collect(word, location);
			}
		}
		
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		private Text index = new Text();
		@Override
		public void reduce(Text key, Iterator<Text> value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			StringBuffer tmp = new StringBuffer();
			String pre = new String();
			boolean first = true;
			while (value.hasNext()){
				String tmp2 = value.next().toString();
				if (!tmp2.equals(pre)){
					pre = tmp2;
					if (first){
						first = false;
					}else {
						tmp.append(',');
					}
					tmp.append(tmp2);
					
				}
			}
			
			index.set(tmp.toString());
			output.collect(key, index);
		}
		
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		JobConf conf = new JobConf(InvertIndex.class);
		conf.setJobName("invertindex");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileSystem.get(conf).delete(new Path(args[1]), true);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
	}

}
