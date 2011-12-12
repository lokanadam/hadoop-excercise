import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

@SuppressWarnings("deprecation")
public class MarkData {

	public final static String MARKDATA = "MarkData";
	public final static String CONFIG = "CONFIG";
	public final static String SUFFIX = "/part-00000";

	public static class MarkDataMapper extends MapReduceBase implements
			Mapper<Text, Text, Text, Text> {

		private static ArrayList<Movie> capopyCenters = new ArrayList<Movie>();

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
							true);
					capopyCenters.add(curr);
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

			String movie_id = key.toString();
			String data = ((Text) values).toString();
			Movie curr = new Movie(movie_id, data, true);
			StringBuilder builder = new StringBuilder();
			builder.append(data);

			for (Movie center : capopyCenters) {

				int matchCount = curr.commonUserCount(center);

				if (matchCount > 2) {
					builder.append(Movie.CANOPYPLITER);
					builder.append(center.id);
				}
			}

			output.collect(key, new Text(builder.toString()));
		}
	}

	public static void run(String input, String config, String output)
			throws IOException {

		JobConf conf = new JobConf(CanopySelection.class);
		conf.setJobName(MARKDATA);
		conf.set(CONFIG, config);

		conf.setMapperClass(MarkDataMapper.class);
		conf.setNumMapTasks(80);

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

		if (args.length < 3) {
			System.out.println("Usage: MarkData "
					+ "<input path> <config path> <output path>");
			System.exit(0);
		}

		run(args[0], args[1], args[2]);
	}
}
