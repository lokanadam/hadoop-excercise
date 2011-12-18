import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.util.*;

public class KMeansIter {
	public static class CenterMap extends MapReduceBase implements Mapper<IntWritable, Text, IntWritable, Text>{
		public void map(IntWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			output.collect(key, new Text("center"));
		}
	}

	
	public static class CenterReduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			boolean isCenter = false;
			String property = "";
			while( values.hasNext() ){ String tmp = values.next().toString();	
				if (tmp.equals("center"))
					isCenter = true;
				else
					property = tmp;
			}
			if ( isCenter )
				output.collect(key, new Text(property));
		}
	
	}
	public static class ClusterMap extends MapReduceBase implements Mapper<IntWritable, Text, IntWritable, Text> {
		private HashMap<Integer, String> centers = new HashMap<Integer, String>();

		@Override
		public void configure(JobConf conf) {
			try {
				FileSystem fs = FileSystem.get(conf);
				Path path = new Path(conf.get("center")+"/part-00000");
				SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
				IntWritable key = new IntWritable();
				Text value = new Text();

				while (reader.next(key, value)) 
					centers.put(key.get(), value.toString());
				
				reader.close();
			}
			catch (Exception e){
				e.printStackTrace();
			}
		}	

		public void map(IntWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			double min = 2.0;
			int minCenter = -1;
			String dataProperty = value.toString();
			String dataTuple[] = dataProperty.split(";");

			for( Map.Entry<Integer, String> entry : centers.entrySet() ) {

				if ( entry.getKey().equals(key.get()) ){
					output.collect(key, new Text(dataTuple[0]+dataTuple[1]+";"+key.get()+":"+0.0));
					return;
				}
				String centerProperty = entry.getValue();	

				String centerTuple[] = centerProperty.split(";");
				
				boolean isSameCanopy = false;

				for (String a : centerTuple[1].split(" "))
					for (String b : dataTuple[1].split(" ")){
						if ( a.equals(b) ){
							isSameCanopy = true;
							break;
						}
					}	

				if ( isSameCanopy ){
					double distance = cosineDistance(centerTuple[0], dataTuple[0]);
					if ( min > distance ){
						min = distance;
						minCenter = entry.getKey();
					}
				}
				reporter.setStatus("I am alive");
			}


			output.collect(key, new Text(dataTuple[0]+dataTuple[1]+";"+minCenter+":"+min));
		}
		private double cosineDistance(String A, String B){
			double SquareA = 0.0;
			double SquareB = 0.0;
			double cross = 0.0;

			for( String item : A.split(","))
				SquareA += Math.pow(Integer.parseInt(item.split(":")[1]), 2);
			for( String item : B.split(","))
				SquareB += Math.pow(Integer.parseInt(item.split(":")[1]), 2);
			
			for( String itemA : A.split(","))
				for( String itemB : B.split(",")){
					String tupleA[] = itemA.split(":");
					String tupleB[] = itemB.split(":");
					if( tupleA[0].equals(tupleB[0]))
						cross += Integer.parseInt(tupleA[1]) * Integer.parseInt(tupleB[1]);
				}

			return 1.0 - cross / ( Math.sqrt(SquareA) * Math.sqrt(SquareB));
		}

	}

	public static class ClusterReduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
		}
	}
	public static class SelectMap extends MapReduceBase implements Mapper<IntWritable, Text, IntWritable, Text> {
		public void map(IntWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException{
			 
			String tuple[] = value.toString().split(";");	

			output.collect( new IntWritable(Integer.parseInt(tuple[1].split(":")[0])),
					new Text(key + ":" + tuple[1].split(":")[1]));  
		
		}
	}
	public static class SelectReduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			HashMap<Integer, Double> map = new HashMap<Integer, Double>();
			double avg = 0.0;
			double min = 2.0;
			int count = 0;
			int center = key.get();
			String set = "";

			while( values.hasNext() ){
				String tmp = values.next().toString();
				String tuple[] = tmp.split(":");
				map.put( Integer.parseInt(tuple[0]), Double.parseDouble(tuple[1]));	
				count++;
				set += tuple[0] + " ";
			}

			for( Map.Entry<Integer, Double> entry : map.entrySet()) 
				avg += entry.getValue();
			
			avg = avg / count;	

			for( Map.Entry<Integer, Double> entry : map.entrySet()){
				double abs = Math.abs(avg-entry.getValue());
				if ( abs < min ) {
					min = abs;
					center = entry.getKey();
				}	
			}
			output.collect( new IntWritable(center), new Text(set.trim()));
		}

	}
	public static void runJob(String set, String data, String center, String output, String cluster) throws Exception {
		// find cluster center
		JobConf conf = new JobConf(KMeansIter.class);

		conf.setJobName("KMeans Center");

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		conf.setReducerClass(CenterReduce.class); // only one reducer required

		MultipleInputs.addInputPath(conf, new Path(set), SequenceFileInputFormat.class, CenterMap.class);
		MultipleInputs.addInputPath(conf, new Path(data), SequenceFileInputFormat.class, IdentityMapper.class);
		FileSystem.get(conf).delete(new Path(center), true);
		FileOutputFormat.setOutputPath(conf, new Path(center));
		JobClient.runJob(conf);


		// let cluster begin
		conf = new JobConf(KMeansIter.class);
		conf.setJobName("Kmeans");

		conf.set("center", center);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		conf.setMapperClass(ClusterMap.class);
		conf.setInputFormat(SequenceFileInputFormat.class);

		FileSystem.get(conf).delete(new Path(output), true);
		FileOutputFormat.setOutputPath(conf, new Path(output));
		FileInputFormat.setInputPaths(conf, new Path(data));
		JobClient.runJob(conf);

		//create new center
		conf = new JobConf(KMeansIter.class);
		conf.setJobName("New Center");
		
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		
		conf.setMapperClass(SelectMap.class);
		conf.setReducerClass(SelectReduce.class);
		conf.setInputFormat(SequenceFileInputFormat.class);
		
		FileSystem.get(conf).delete(new Path(cluster), true);
		FileOutputFormat.setOutputPath(conf, new Path(cluster));
		FileInputFormat.setInputPaths(conf, new Path(output));
		JobClient.runJob(conf);
	}
	public static void main(String args[]) throws Exception {
		runJob("/tmp/kmeans/canopy", "/tmp/kmeans/data", "/tmp/kmeans/center", "/tmp/kmeans/iter", "/tmp/kmeans/cluster");
		runJob("/tmp/kmeans/cluster", "/tmp/kmeans/data", "/tmp/kmeans/center", "/tmp/kmeans/iter", "/tmp/kmeans/cluster");
	}
}
