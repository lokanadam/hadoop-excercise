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
		private IntWritable movieid = new IntWritable();
		public void map(IntWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException{
			String set[] = value.toString().split(",");
			for(String movie : set){
				movieid.set(Integer.parseInt(movie));
				output.collect( movieid,
						new Text(key.toString()));
			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
		private MultipleOutputs multipleOutputs;

		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			StringBuffer canopyMark = new StringBuffer();
			String property = "";
			String tmp;
			while(values.hasNext()){
				tmp = values.next().toString();
				if ( tmp.indexOf(':') == -1 ) {
					canopyMark.append(tmp);
					canopyMark.append(" ");
				}
				else
					property = tmp;
			}
			output.collect(key,new Text(property + ";" +canopyMark.toString().trim()));	

		}
	}
	public static void runJob(String data, String canopy, String output) throws Exception {

		JobConf conf = new JobConf(MarkData.class);
		conf.setJobName("MarkData");

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		MultipleInputs.addInputPath(conf, new Path(data), SequenceFileInputFormat.class, IdentityMapper.class);
		MultipleInputs.addInputPath(conf, new Path(canopy), SequenceFileInputFormat.class, CanopyMap.class);
		conf.setReducerClass(Reduce.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		FileSystem.get(conf).delete(new Path(output), true);
		FileOutputFormat.setOutputPath(conf, new Path(output));

		JobClient.runJob(conf);
	}
	public static void main(String args[]) throws Exception {
		runJob(args[0], args[1], args[2]);
	}
}
