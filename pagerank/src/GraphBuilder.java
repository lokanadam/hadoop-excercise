import java.util.*;
import java.util.regex.*;
import java.io.*;
import java.nio.charset.*;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.*;

public class GraphBuilder {
	public static class GraphBuilderMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
		private static final Pattern wikiLinksPattern = Pattern.compile("\\[.+?\\]");
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String[] titleAndText = parseTitleAndText(value);

			// the title
			String pageString = titleAndText[0];
			Text page = new Text(pageString.replace(' ', '_'));

			// the links
			Matcher matcher = wikiLinksPattern.matcher(titleAndText[1]);
			while (matcher.find()) {
				String otherPage = matcher.group();
				// filter only wiki pages
				otherPage = getWikiPageFromLink(otherPage);
				if (otherPage == null || otherPage.isEmpty())
					continue;

				output.collect(page, new Text(otherPage));
			} 
		}	

		private String[] parseTitleAndText(Text value) throws CharacterCodingException {
			String[] titleAndText = new String[2];
			int start = value.find("&lttitle&gt");
			start += 11;
			int end = value.find("&lt/title&gt", start);
		
			titleAndText[0] = Text.decode(value.getBytes(), start, end-start);

			start = value.find("&lttext&gt");
			start += 10;
			end = value.find("&lt/text&gt", start);

			if (start == -1 || end == -1)
				return new String[]{"", ""};

			titleAndText[1] = Text.decode(value.getBytes(), start, end-start);
			return titleAndText;
		}

		private String getWikiPageFromLink(String aLink) {
			if (isNotWikiLink(aLink)) return null;
		
			int start = aLink.startsWith("[[") ? 2 : 1;
			int endLink = aLink.indexOf("]");

			int pipePosition = aLink.indexOf("|");
			if(pipePosition > 0){
			    endLink = pipePosition;
			}
			
			int part = aLink.indexOf("#");
			if(part > 0){
			    endLink = part;
			}
			
			aLink =  aLink.substring(start, endLink);
			aLink = aLink.replaceAll("\\s", "_");
			aLink = aLink.replaceAll(",", "");
			aLink = sweetify(aLink);
			
			return aLink;
		}		

		private String sweetify(String aLinkText) {
			if(aLinkText.contains("&amp;"))
			    return aLinkText.replace("&amp;", "&");

			return aLinkText;
		}

		private boolean isNotWikiLink(String aLink) {
			int start = 1;
			if (aLink.startsWith("[[")){
			    start = 2;
			}
        
			if (aLink.length() < start+2 || aLink.length() > 100) return true;
			char firstChar = aLink.charAt(start);
			
			if (firstChar == '#') return true;
			if (firstChar == ',') return true;
			if (firstChar == '.') return true;
			if (firstChar == '&') return true;
			if (firstChar == '\'') return true;
			if (firstChar == '-') return true;
			if (firstChar == '{') return true;
			
			if (aLink.contains(":")) return true; // Matches: external links and translations links
			if (aLink.contains(",")) return true; // Matches: external links and translations links
			if (aLink.contains("&")) return true;
			
			return false;
		}
	}

	// Input: <fromPage, toPage>
	// Output: <toPage, <pagerank '\t' toPage1,toPage2,...,toPageN>>
	public static class GraphBuilderReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String pagerank = "1.0\t";

			boolean first = true;
			while (values.hasNext()) {
				if(!first) pagerank += ",";

				pagerank += values.next().toString();
				first = false;
			}
			output.collect(key, new Text(pagerank));
		}	
	}
	
	public static void runJob(String inputPath, String outputPath) throws IOException {
		JobConf conf = new JobConf(GraphBuilder.class);	

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		conf.setInputFormat(TextInputFormat.class);
		conf.setMapperClass(GraphBuilderMapper.class);

		FileSystem.get(conf).delete(new Path(outputPath),true);
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setReducerClass(GraphBuilderReducer.class);

		JobClient.runJob(conf);
	}

	public static void main(String args[]) throws Exception {
		runJob(args[0], args[1]);
	}
}
