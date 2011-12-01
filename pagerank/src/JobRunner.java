import java.util.*;
import java.io.*;


public class JobRunner {
	public static int itrnum;
	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException{
		itrnum =5;
		runGraphBuilder();
		runPageRankIterator();
		runPageRankViewer();
	}
	
	public static void runGraphBuilder()throws IOException{
		GraphBuilder.runJob("pagerank/enwiki", "pagerank/itr0");
	}
	
	public static void runPageRankIterator() throws IOException{
		for (int i=0;i<itrnum;i++)
			PageRankIterator.runJob("pagerank/itr"+i, "pagerank/itr"+String.valueOf(i+1));
	}
	
	public static void runPageRankViewer() throws IOException{
		PageRankViewer.runJob("pagerank/itr"+itrnum, "pagerank/output");
	}
}
