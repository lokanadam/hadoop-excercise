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
			String minCenter = key.toString();
			String dataProperty = value.toString();
			String dataTuple[] = dataProperty.split(";");

			for( String canopy : dataTuple[1].split(" ")){
				String centerProperty = centers.get(Integer.parseInt(canopy));
				String centerTuple[] = centerProperty.split(";");
				double distance = cosineDistance(centerTuple[0], dataTuple[0]);
				if ( min > distance ){
					min = distance;
					minCenter = canopy;
				}
				reporter.setStatus("I am alive");
			}
			output.collect(new IntWritable(Integer.parseInt(minCenter)), new Text(key.get()+";"+dataTuple[0]));
		}

		private double cosineDistance(String A, String B){
			double SquareA = 0.0;
			double SquareB = 0.0;
			double cross = 0.0;

			for( String item : A.split(","))
				SquareA += Math.pow(Double.parseDouble(item.split(":")[1]), 2);
			for( String item : B.split(","))
				SquareB += Math.pow(Double.parseDouble(item.split(":")[1]), 2);
			
			for( String itemA : A.split(","))
				for( String itemB : B.split(",")){
					String tupleA[] = itemA.split(":");
					String tupleB[] = itemB.split(":");
					if( tupleA[0].equals(tupleB[0]))
						cross += Double.parseDouble(tupleA[1]) * Double.parseDouble(tupleB[1]);
				}

			return 1.0 - cross / ( Math.sqrt(SquareA) * Math.sqrt(SquareB));
		}

	}

	public static class ClusterReduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			HashMap<String, Rating> map = new HashMap<String, Rating>();
			String cluster = "";
			while(values.hasNext()){
				String tmp = values.next().toString();
				String tuple[] = tmp.split(";");
				String userRatings[] = tuple[1].split(",");
				cluster += tuple[0]+" ";

				for(String userRate : userRatings ){
					String user = userRate.split(":")[0];
					String rate = userRate.split(":")[1];
					if ( map.containsKey(user)) {
						Rating rating = map.get(user);
						rating.merge(rate);
						map.put(user, rating);
					}
					else {
						Rating rating = new Rating(user+rate);
						map.put(user, rating);
					}
				}
			}

			ArrayList<Rating> list = new ArrayList<Rating>(map.values());
			Collections.sort(list);

			for( Rating rating : list ){
				if ( map.size() < 100000 )
					break;
				else
					map.remove(rating.user);
			}

			String property = "";
			for( Map.Entry<String, Rating> entry : map.entrySet() )
				property += ","+entry.getKey()+":"+entry.getValue().avg();

			property = property.substring(1);

			output.collect(key, new Text(property + ";"+ cluster.trim()));
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
			conf.setOutputKeyClass(IntWritable.class);
			conf.setOutputValueClass(Text.class);
			conf.setOutputFormat(SequenceFileOutputFormat.class);

			conf.setMapperClass(ClusterMap.class);
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
