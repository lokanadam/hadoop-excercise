import java.util.*;
import java.io.*;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.*;

public class JobRunner {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException{
		runGraphBuilder();
	}
	
	public static void runGraphBuilder(){
		GraphBuilder.runJob("", "");
	}
}
