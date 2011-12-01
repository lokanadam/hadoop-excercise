import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

public class PageRankViewer {
	public static class PageRankViewerMapper extends MapReduceBase implements Mapper<LongWritable, Text, FloatWritable, Text> {
		private Text outPage = new Text();
		private FloatWritable outPr = new FloatWritable();
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<FloatWritable, Text> output, Reporter reporter)
				throws IOException {
			String[] line = value.toString().split("\t");
			String page = line[0];
			float pr = Float.parseFloat(line[1]);
			outPage.set(page);
			outPr.set(pr);
			output.collect(outPr, outPage);
		}
	}
	public static class DescendingComparator extends FloatWritable.Comparator{
		public float compare(WritableComparator a, WritableComparable b){
			return -super.compare(a, b);
		}
		public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4, int arg5){
			return -super.compare(arg0, arg1, arg2, arg3, arg4, arg5);
		}
	}
	
	public static void runJob(String inputPath, String outputPath) throws IOException {
		JobConf conf = new JobConf(PageRankViewer.class);
		conf.setJobName("PageRankView");
		
		conf.setOutputKeyClass(FloatWritable.class);
		conf.setOutputKeyComparatorClass(DescendingComparator.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(PageRankViewerMapper.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileSystem.get(conf).delete(new Path(outputPath), true);
		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		
		JobClient.runJob(conf);
	}
}
