import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

@SuppressWarnings("deprecation")
public class CanopySelection {

	public final static String CANOPYSELECTION = "CanopySelection";

	public static class CanopySelectionMapper extends MapReduceBase implements
			Mapper<Text, Text, Text, Text> {

		private ArrayList<Movie> canopyCenters;

		@Override
		public void configure(JobConf job) {
			this.canopyCenters = new ArrayList<Movie>();
		}

		@Override
		public void map(Text key, Text values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String id = key.toString();
			String data = values.toString();
			Movie curr = new Movie(id, data, true);

			boolean included = false;

			for (Movie movie : canopyCenters) {
				if (movie.commonUserCount(curr) > 8) {
					included = true;
					break;
				}
			}

			if (!included) {
				output.collect(key, values);
				this.canopyCenters.add(curr);
			}
		}
	}

	public static void run(String input, String output) throws IOException {

		JobConf conf = new JobConf(CanopySelection.class);
		conf.setJobName(CANOPYSELECTION);

		conf.setMapperClass(CanopySelectionMapper.class);

		conf.setInputFormat(SequenceFileInputFormat.class);
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
			System.out
					.println("Usage: CanopySelection <input path> <output path>");
			System.exit(0);
		}

		run(args[0], args[1]);
	}
}
