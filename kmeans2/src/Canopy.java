import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Canopy {
	public static class IndexInvertMap extends MapReduceBase implements Mapper<IntWritable, Text, Text, Text> {
		private Text user = new Text();
		private Text movie = new Text();
		public void map(IntWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String line = value.toString();
			StringTokenizer tokens = new StringTokenizer(line, ",");
			while(tokens.hasMoreTokens()){
				user.set(tokens.nextToken().split(":")[0]);
				movie.set(key.toString());
				output.collect(user, movie);
			}
		}

	}

	public static class IndexInvertReduce extends MapReduceBase implements Reducer<Text, Text, LongWritable, Text> {
		private Text mList = new Text();
		private LongWritable user = new LongWritable();
		public void reduce(Text key, Iterator<Text> values, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			user.set(Long.parseLong(key.toString()));
			
			String tmp = "";
			while(values.hasNext()){
				tmp += values.next().toString();
				if ( values.hasNext() )
					tmp +=" ";
			}

			mList.set(tmp);

			output.collect(user, mList);	
		}
	}

	public static class CanopyMap extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, IntWritable> {
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
			String movies[] = value.toString().split(" ");
			for( int i = 0 ; i < movies.length ; i++ )
				for( int j = 0 ; j < movies.length ; j++ ){
					if ( i != j )
						output.collect( new IntWritable(Integer.parseInt(movies[i])),
								new IntWritable(Integer.parseInt(movies[j])));
				}
		}
	}
	
	public static class CanopyReduce extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, Text> {
		private Set<Integer> points;
		public void configure(JobConf conf){
			points = new HashSet<Integer>();	
			for( int i = 0 ; i < Integer.parseInt(conf.get("movie number")); i++ )
				points.add(i);
		}
		public void reduce(IntWritable key, Iterator<IntWritable> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			if (points.contains(key.get())){
				reporter.setStatus("got key "+key.get());
				points.remove(key.get());
		
				Map<Integer, Integer> map = new HashMap<Integer, Integer>();
		
				while( values.hasNext() ){
					int i = values.next().get();
					if( !points.contains(i) )
						continue;
					else if( map.containsKey(i) && map.get(i) == 8)
						points.remove(i);
					else if( map.containsKey(i) )
						map.put(i, map.get(i) + 1);
					else
						map.put(i, 1);
				}
				
				String canopy = key.toString();
				for(Map.Entry<Integer, Integer> entry : map.entrySet()){
					if( entry.getValue() >= 2)
						canopy += "," + entry.getKey();
				}
				output.collect(key, new Text(canopy));
			}	
		}
	}
	public static void runJob(String input, String output, String numMovie) throws Exception {

		JobConf iconf = new JobConf(Canopy.class);
		iconf.setJobName("IndexInvert");

		iconf.setMapOutputKeyClass(Text.class);
		iconf.setMapOutputValueClass(Text.class);

		iconf.setOutputKeyClass(LongWritable.class);
		iconf.setOutputValueClass(Text.class);

		iconf.setMapperClass(IndexInvertMap.class);
		iconf.setReducerClass(IndexInvertReduce.class);

		iconf.setInputFormat(SequenceFileInputFormat.class);
		iconf.setOutputFormat(SequenceFileOutputFormat.class);
		FileSystem.get(iconf).delete(new Path("/kmeans/indexinvert"), true);

		FileInputFormat.setInputPaths(iconf, new Path(input));
		FileOutputFormat.setOutputPath(iconf, new Path("/kmeans/indexinvert"));

		JobClient.runJob(iconf);


		// canopy select
		
		JobConf cconf = new JobConf(Canopy.class);
		cconf.setJobName("Canopy");

		cconf.set("movie number", numMovie);

		cconf.setOutputKeyClass(IntWritable.class);
		cconf.setOutputValueClass(Text.class);

		cconf.setMapOutputValueClass(IntWritable.class);
		cconf.setMapperClass(CanopyMap.class);
		cconf.setReducerClass(CanopyReduce.class);

		cconf.setInputFormat(SequenceFileInputFormat.class);
		cconf.setOutputFormat(SequenceFileOutputFormat.class);
		FileSystem.get(cconf).delete(new Path(output), true);

		FileInputFormat.setInputPaths(cconf, new Path("/kmeans/indexinvert"));
		FileOutputFormat.setOutputPath(cconf, new Path(output));

		JobClient.runJob(cconf);
		
	}
	public static void main(String args[]) throws Exception {
		runJob(args[0], args[1], args[2]);
	}
}
