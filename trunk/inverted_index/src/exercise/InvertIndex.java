package exercise;

import java.util.*;
import java.io.*;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.*;

public class InvertIndex {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		private Text location = new Text();
		private HashSet<String> dict = new HashSet<String>();
		private final static int numCommonWords = 6000; 
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			FileSplit fileSplit = (FileSplit)reporter.getInputSplit();
			String fileName = fileSplit.getPath().getName();
			location.set(fileName);
			if (dict.isEmpty()){
				readDict();
			}
			while (tokenizer.hasMoreTokens()){
				String word_str = scrub(tokenizer.nextToken());
				if (!dict.contains(word_str)) {
					word.set(word_str);
					output.collect(word, location);
				}
			}
		}
		private String scrub(String noisyword) {			
			return noisyword.toLowerCase().replaceAll("\\pP|\\pS", "");
		}
		private void readDict() throws IOException {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			InputStream in = null;
			try {
				int countNum = 0;
				in = fs.open(new Path("/dict"));				
				Scanner sc = new Scanner(in);				
				while (sc.hasNext() && countNum < numCommonWords) {					
					//if (sc.nextInt()>5) {
					String dictword = scrub(sc.next());
					dict.add(dictword);
					countNum ++;
					//}					
				}				
			}finally {
				IOUtils.closeStream(in);
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
