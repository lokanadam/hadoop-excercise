import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

@SuppressWarnings("deprecation")
public class DataPrep {

	public final static String DATAPREP = "DataPrep";

	public static class DataPrepMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private static String movieID = null;

		@Override
		public void map(LongWritable key, Text values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String[] tokens = values.toString().split(Movie.UIDSPLITER);

			if (tokens.length == 1) {
				if (!tokens[0].endsWith(Movie.MIDSPLITER)) {
					return;
				}

				movieID = tokens[0].substring(0, tokens[0].length() - 1);
				return;
			} else if (tokens.length != 3 || movieID == null) {
				return;
			}

			Text keyToEmit = new Text(movieID);
			Text valueToEmit = new Text(tokens[0] + Movie.UIDSPLITER
					+ tokens[1]);
			output.collect(keyToEmit, valueToEmit);
		}
	}

	public static class DataPrepReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			TreeMap<Integer, String> ratings = new TreeMap<Integer, String>();

			while (values.hasNext()) {
				String[] items = values.next().toString()
						.split(Movie.UIDSPLITER);
				ratings.put(new Integer(items[0]), items[1]);
			}

			StringBuilder builder = new StringBuilder();

			for (Integer userID : ratings.keySet()) {

				builder.append(userID.toString());
				builder.append(Movie.UIDSPLITER);
				builder.append(ratings.get(userID));
				builder.append(Movie.U_RSPLITER);
			}

			if (builder.length() > 0) {
				builder.deleteCharAt(builder.length() - 1);
			}

			output.collect(key, new Text(builder.toString()));
		}
	}

	public static void run(String input, String output) throws IOException {

		JobConf conf = new JobConf(DataPrep.class);
		conf.setJobName(DATAPREP);

		conf.setMapperClass(DataPrepMapper.class);
		conf.setReducerClass(DataPrepReducer.class);
		conf.setNumMapTasks(1);
		conf.setNumReduceTasks(80);

		conf.setInputFormat(TextInputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(input));

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		FileSystem.get(conf).delete(new Path(output), true);
		FileOutputFormat.setOutputPath(conf, new Path(output));

		JobClient.runJob(conf);
	}

	public static void main(String[] args) throws IOException {

		if (args.length < 2) {
			System.out.println("Usage: DataPrep <input path> <output path>");
			System.exit(0);
		}

		run(args[0], args[1]);
	}
}
