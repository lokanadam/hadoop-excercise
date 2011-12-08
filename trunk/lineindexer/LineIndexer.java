import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class LineIndexer {
	public static class LineIndexerMapper extends MapReduceBase implements Mapper<WritableComparable, Writable, WritableComparable, Text> {
		private final static Text word = new Text();
		private final static Text summary = new Text();
		public void map(WritableComparable key, Writable val, OutputCollector<WritableComparable, Text> output, Reporter reporter) throws IOException {
			String line = val.toString();
			summary.set(key.toString() + ":" + line);
			StringTokenizer itr = new StringTokenizer(line.toLowerCase());
			while(itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				output.collect(word, summary);
			}
		}
	}

	public static class LineIndexerReducer extends MapReduceBase implements Reducer<WritableComparable, Text, WritableComparable, Text> {
		public void reduce(WritableComparable key, Iterator<Text> values, OutputCollector<WritableComparable, Text> output, Reporter reporter) throws IOException {
			boolean first = true;
			StringBuilder toReturn = new StringBuilder();
			while(values.hasNext()){
				if(!first)
					toReturn.append('^');
				first=false;
				toReturn.append(values.next().toString());
			}
			output.collect(key, new Text(toReturn.toString()));
		}
	}

	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf(LineIndexer.class);
		conf.setJobName("LineIndexer");
		// The keys are words (strings):
		conf.setOutputKeyClass(Text.class);
		// The values are offsets+line (strings):
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(LineIndexerMapper.class);
		conf.setReducerClass(LineIndexerReducer.class);
		if (args.length < 2) {
			System.out.println("Usage: LineIndexer <input path> <output path>");
			System.exit(0);
		}
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
	}
}
