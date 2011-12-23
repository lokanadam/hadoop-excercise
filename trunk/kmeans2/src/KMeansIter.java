import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.util.*;

public class KMeansIter {
	
	public static class CenterReduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			boolean isCenter = false;
			String property = "";
			String set = "";
			while( values.hasNext() ){ 
				String tmp = values.next().toString();	
				if (tmp.indexOf(':') == -1){
					isCenter = true;
					set = tmp;
				}
				else
					property = tmp.split(";")[0];
			}
			if ( isCenter )
				output.collect(key, new Text(property+";"+set));
		}
	
	}
	public static class ClusterMap extends MapReduceBase implements Mapper<IntWritable, Text, IntWritable, Text> {
		private HashMap<Integer, HashMap<String, Double>> centers = new HashMap<Integer, HashMap<String, Double>>();

		@Override
		public void configure(JobConf conf) {
			try {
				FileSystem fs = FileSystem.get(conf);
				Path path = new Path(conf.get("center")+"/part-00000");
				SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
				IntWritable key = new IntWritable();
				Text value = new Text();

				while (reader.next(key, value)) {
					HashMap<String, Double> ratings = new HashMap<String, Double>(); 
					for( String rating : value.toString().split(";")[0].split(",") ){
						String rTuple[] = rating.split(":");
						ratings.put(rTuple[0], Double.parseDouble(rTuple[1]));
					}
					centers.put(key.get(), ratings);
				}
				
				reader.close();
			}
			catch (Exception e){
				e.printStackTrace();
			}
		}	

		public void map(IntWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			double min = 2.0;
			int count = 0;
			String minCenter = key.toString();
			String dataProperty = value.toString();
			String dataTuple[] = dataProperty.split(";");
			
			HashMap<String, Double> map = new HashMap<String, Double>();
			for( String rating : dataTuple[0].split(",")){
				String rTuple[] = rating.split(":");
				map.put(rTuple[0], Double.parseDouble(rTuple[1]));
			}
			for( String canopy : dataTuple[1].split(" ")){
				double distance = cosineDistance(centers.get(Integer.parseInt(canopy)), map);
				if ( min > distance ){
					min = distance;
					minCenter = canopy;
				}
				reporter.setStatus("Map: I am alive "+ count++);
			}
			output.collect(new IntWritable(Integer.parseInt(minCenter)), new Text(key.get()+";"+dataTuple[0]));
		}

		private double cosineDistance(HashMap<String,Double> A, HashMap<String,Double> B){
			double SquareA = 0.0;
			double SquareB = 0.0;
			double cross = 0.0;
			boolean firstInner = false;

			Set<Map.Entry<String, Double>> setA = A.entrySet();
			Set<Map.Entry<String, Double>> setB = B.entrySet();
			for( Map.Entry<String, Double> entryA : setA )
				SquareA += Math.pow(entryA.getValue(), 2);
			for( Map.Entry<String, Double> entryB : setB )
				SquareB += Math.pow(entryB.getValue(), 2);
			
			for( Map.Entry<String, Double> entryA : setA )
				if ( B.containsKey(entryA.getKey()))
					cross += entryA.getValue() * B.get(entryA.getKey());	

			return 1.0 - cross / ( Math.sqrt(SquareA) * Math.sqrt(SquareB));
		}

	}

	public static class ClusterReduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			HashMap<String, Rating> map = new HashMap<String, Rating>();
			StringBuffer cluster = new StringBuffer();
			while(values.hasNext()){
				String tmp = values.next().toString();
				String tuple[] = tmp.split(";");
				String userRatings[] = tuple[1].split(",");
				cluster.append(tuple[0]);
				if ( values.hasNext() )
					cluster.append(" ");

				for(String userRate : userRatings ){
					String user = userRate.split(":")[0];
					String rate = userRate.split(":")[1];
					if ( map.containsKey(user)) {
						map.get(user).merge(rate);
					}
					else {
						Rating rating = new Rating(user+":"+rate);
						map.put(user, rating);
					}
					reporter.setStatus("I am alive");
				}
			}

			ArrayList<Rating> list = new ArrayList<Rating>(map.values());
			reporter.setStatus("init list");
			Collections.sort(list);
			reporter.setStatus("after sort");

			for( Rating rating : list ){
				if ( map.size() < 1000 )
					break;
				else
					map.remove(rating.user);
				reporter.setStatus("remove movies");
			}

			StringBuffer property = new StringBuffer();
			int map_size = map.size();
			int count = 0;
			for( Map.Entry<String, Rating> entry : map.entrySet() ){
				property.append(entry.getKey());
				property.append(":");
				property.append(entry.getValue().avg());
				if (++count < map_size )
					property.append(",");
				
				reporter.setStatus("I am alive");
			}

			output.collect(key, new Text(property.toString()+";"+cluster.toString()));
		}
	}
	public static void runJob(String data, String set, String iterPath , int iterNum ) throws Exception {
		// find cluster center
		JobConf conf = new JobConf(KMeansIter.class);

		conf.setJobName("KMeans Center");

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		conf.setReducerClass(CenterReduce.class); // only one reducer required

		MultipleInputs.addInputPath(conf, new Path(set), SequenceFileInputFormat.class, IdentityMapper.class);
		MultipleInputs.addInputPath(conf, new Path(data), SequenceFileInputFormat.class, IdentityMapper.class);
		FileSystem.get(conf).delete(new Path(iterPath+0), true);
		FileOutputFormat.setOutputPath(conf, new Path(iterPath+0));
		JobClient.runJob(conf);


		for( int i = 1 ; i <= iterNum ; i++ ){
			// let cluster begin
			conf = new JobConf(KMeansIter.class);
			conf.setJobName("Kmeans"+i);

			conf.set("center", iterPath+(i-1));
			if ( i == iterNum )
				conf.set("stage", "final");
			else
				conf.set("stage", "iter");
			conf.setOutputKeyClass(IntWritable.class);
			conf.setOutputValueClass(Text.class);
			conf.setOutputFormat(SequenceFileOutputFormat.class);

			conf.setMapperClass(ClusterMap.class);
			conf.setReducerClass(ClusterReduce.class);
			conf.setInputFormat(SequenceFileInputFormat.class);

			FileSystem.get(conf).delete(new Path(iterPath+i), true);
			FileOutputFormat.setOutputPath(conf, new Path(iterPath+i));
			FileInputFormat.setInputPaths(conf, new Path(data));
			JobClient.runJob(conf);
		}
	}
	public static void main(String args[]) throws Exception {
	}
}
