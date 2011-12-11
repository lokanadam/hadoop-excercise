package exercise;

import java.util.*;
import java.io.*;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.*;

public class QueryWord {	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		private JobConf conf;

		@Override
		public void configure(JobConf conf) { this.conf = conf; }

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] tokens = line.split("\t");

			if (tokens[0].equals(conf.get("keyword")))			
				output.collect(new Text(tokens[0]), new Text(tokens[1]));
		}
	}

	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf(QueryWord.class);
		FileSystem fs = FileSystem.get(conf);

		conf.setJobName("queryword");
		conf.setMapperClass(Map.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		// read the keyword
		Scanner s = new Scanner(System.in);
		System.out.println("Please input the word to query:");
		String word = s.next();
		conf.set("keyword", word);

		fs.delete(new Path(args[1]), true);

		JobClient.runJob(conf);

		// print the result
		System.out.println("The result is:");
		s = new Scanner(fs.open(new Path(args[1] + "/part-00000")));
		s.next();
		System.out.println(s.next());
	}

}
