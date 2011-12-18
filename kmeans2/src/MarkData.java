import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.util.*;

public class MarkData {
	public static class CanopyMap extends MapReduceBase implements Mapper<IntWritable, Text, IntWritable, Text> {
		public void map(IntWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException{
			String set[] = value.toString().split(",");
			for(String movie : set){
				output.collect( new IntWritable(Integer.parseInt(movie)),
						new Text(key.toString()));
			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
		private MultipleOutputs multipleOutputs;

		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			String canopyMark = "";
			String property = "";
			String tmp;
			while(values.hasNext()){
				tmp = values.next().toString();
				if ( tmp.indexOf(':') == -1 ) {
					canopyMark += tmp + " ";
				}
				else
					property = tmp;
			}
			output.collect(key,new Text(property + ";" +canopyMark.trim()));	

		}
	}
	public static void runJob(String input, String output) throws Exception {

		JobConf conf = new JobConf(MarkData.class);
		conf.setJobName("MarkData");

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		MultipleInputs.addInputPath(conf, new Path("/tmp/kmeans/dataprepare"), SequenceFileInputFormat.class, IdentityMapper.class);
		MultipleInputs.addInputPath(conf, new Path(input), SequenceFileInputFormat.class, CanopyMap.class);
		conf.setReducerClass(Reduce.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		FileSystem.get(conf).delete(new Path(output), true);
		FileOutputFormat.setOutputPath(conf, new Path(output));

		JobClient.runJob(conf);
	}
	public static void main(String args[]) throws Exception {
		runJob(args[0], args[1]);
	}
}
