import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

@SuppressWarnings("deprecation")
public class KMeans {

	public final static String KMEANS = "KMeans";
	public final static String OUTPUT = "Output";
	public final static String CONFIG = "CONFIG";
	public final static String SUFFIX = "/part-00000";

	public final static int ITERATION = 5;
	public final static int THRESHOLD = 50;

	public static class KMeansMapper extends MapReduceBase implements
			Mapper<Text, Text, Text, Text> {

		private static HashMap<String, Movie> kmeansCenters = new HashMap<String, Movie>();

		@Override
		public void configure(JobConf conf) {

			try {

				FileSystem fs = FileSystem.get(conf);
				Path path = new Path(conf.get(CONFIG) + SUFFIX);
				SequenceFile.Reader reader = new SequenceFile.Reader(fs, path,
						conf);
				Text key = new Text();
				Text value = new Text();

				while (true) {

					reader.next(key, value);

					if (key.toString().equals(""))
						break;

					Movie curr = new Movie(key.toString(), value.toString(),
							false);
					kmeansCenters.put(key.toString(), curr);
					key.set("");
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void map(Text key, Text values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String[] items = values.toString().split(Movie.CANOPYPLITER);

			Movie curr = new Movie(key.toString(), items[0], false), closest = null;

			double weight = -1, temp;

			for (int i = 1; i < items.length; i++) {

				Movie center = kmeansCenters.get(items[i]);

				if (center == null) {
					continue;
				}

				temp = curr.cosineDistance(center);

				if (weight < temp) {
					closest = center;
					weight = temp;
				}
			}

			if (weight > -1) {
				output.collect(new Text(closest.id), new Text(key.toString()
						+ Movie.MIDSPLITER + items[0]));
			}
		}
	}

	public static class KMeansReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			TreeMap<Integer, Rating> ratings = new TreeMap<Integer, Rating>();

			while (values.hasNext()) {

				String data = values.next().toString();
				String[] usersWithRating = data.split(Movie.MIDSPLITER)[1]
						.split(Movie.U_RSPLITER);

				for (String ratingdata : usersWithRating) {

					Rating rating = new Rating(ratingdata, false);

					if (ratings.containsKey(rating.uid)) {
						ratings.get(rating.uid).merge(rating);
					} else {
						ratings.put(rating.uid, rating);
					}
				}
			}

			StringBuilder builder = new StringBuilder();
			ArrayList<Rating> list = new ArrayList<Rating>(ratings.values());
			Collections.sort(list);

			for (Rating rating : list) {

				if (ratings.size() < THRESHOLD) {
					break;
				}

				ratings.remove(rating.uid);
			}

			for (Integer id : ratings.keySet()) {

				Rating rating = ratings.get(id);
				builder.append(id);
				builder.append(Movie.UIDSPLITER);
				builder.append((rating.rating / rating.count));
				builder.append(Movie.U_RSPLITER);
			}

			if (builder.length() > 0) {
				builder.deleteCharAt(builder.length() - 1);
			}

			output.collect(key, new Text(builder.toString()));
		}
	}

	public static class Viewer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		private static int index = 0;

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			index++;
			int current = index;

			TreeSet<Integer> cluster = new TreeSet<Integer>();

			while (values.hasNext()) {

				String data = values.next().toString();
				cluster.add(new Integer(data.split(Movie.MIDSPLITER)[0]));
			}

			StringBuilder builder = new StringBuilder();

			for (Integer mid : cluster) {

				builder.append(mid);
				builder.append(", ");
			}

			if (builder.length() > 1) {
				builder.delete(builder.length() - 2, builder.length());
			}

			output.collect(new Text("Cluster" + current),
					new Text(builder.toString()));
		}
	}

	public static void runKMeans(String input, String config, String output)
			throws IOException {

		JobConf conf = new JobConf(KMeans.class);
		conf.setJobName(KMEANS);
		conf.set(CONFIG, config);

		conf.setMapperClass(KMeansMapper.class);
		conf.setReducerClass(KMeansReducer.class);

		conf.setNumReduceTasks(10);

		conf.setInputFormat(SequenceFileInputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(input));

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		FileSystem.get(conf).delete(new Path(output), true);
		FileOutputFormat.setOutputPath(conf, new Path(output));

		JobClient.runJob(conf);
	}

	public static void output(String input, String config, String output)
			throws IOException {

		JobConf conf = new JobConf(KMeans.class);
		conf.setJobName(OUTPUT);
		conf.set(CONFIG, config);

		conf.setMapperClass(KMeansMapper.class);
		conf.setReducerClass(Viewer.class);

		conf.setInputFormat(SequenceFileInputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(input));

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileSystem.get(conf).delete(new Path(output), true);
		FileOutputFormat.setOutputPath(conf, new Path(output));

		JobClient.runJob(conf);
	}

	public static void run(String input, String config, String tmp,
			String output) throws IOException {
		runKMeans(input, config, tmp + "/0");

		for (int i = 1; i < ITERATION; i++) {
			runKMeans(input, tmp + "/" + (i - 1), tmp + "/" + i);
		}

		output(input, tmp + "/" + (ITERATION - 1), output);
	}

	public static void main(String[] args) throws IOException {

		if (args.length < 3) {
			System.out.println("Usage: KMeansFromCanopy "
					+ "<input path> <config path> <temp path> <output path> ");
			System.exit(0);
		}

		run(args[0], args[1], args[2], args[3]);
	}
}
