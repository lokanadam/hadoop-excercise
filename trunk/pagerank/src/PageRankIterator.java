import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

public class PageRankIterator {
	public static final double dample = 0.85;
	public static class PageRankIteratorMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] tuple = line.split("\t");
			String pageKey = tuple[0];
			double pr = Double.parseDouble(tuple[1]);
			if (tuple.length>2) {
				String[] linkPages = tuple[2].split(",");
				for (String linkPage : linkPages) {				
					//String prValue = pageKey + " "+tuple[1]+" "+String.valueOf(linkPages.length);
					String prValue = pageKey + "\t" + String.valueOf(pr/linkPages.length);
					output.collect(new Text(linkPage), new Text(prValue));
				}
			
				output.collect(new Text(pageKey), new Text("|"+tuple[2]));
			}
		}		
	}
	
	public static class PageRankIteratorReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String links = "";
			double pagerank = 0;
			while (value.hasNext()) {
				String tmp = value.next().toString();
				
				if (tmp.startsWith("|")) {
					links = "\t"+tmp.substring(tmp.indexOf("|")+1);
					continue;
				}
				
				String[] tuple = tmp.split("\t");
				if (tuple.length>1)
					pagerank += Double.parseDouble(tuple[1]);
			}
			pagerank = (1-dample) + dample*pagerank;
			output.collect(key, new Text(String.valueOf(pagerank)+links));
		}
		
	}
	
	public static void runJob(String inputPath, String outputPath) throws IOException{
		JobConf conf = new JobConf(PageRankIterator.class);
		conf.setJobName("PageRankItr");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(PageRankIteratorMapper.class);		
		conf.setReducerClass(PageRankIteratorReducer.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileSystem.get(conf).delete(new Path(outputPath), true);
		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		
		JobClient.runJob(conf);
	}
}
