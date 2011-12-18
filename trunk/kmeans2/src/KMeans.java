public class KMeans{

	public static void main(String args[]) throws Exception{
		DataPrepare.runJob(args[0], "/tmp/kmeans/dataprepare");
		Canopy.runJob("/tmp/kmeans/dataprepare","/tmp/kmeans/canopy");
		MarkData.runJob("/tmp/kmeans/canopy","/tmp/kmeans/data");
		KMeansIter.runJob("/tmp/kmeans/canopy", "/tmp/kmeans/data", "/tmp/kmeans/center", "/tmp/kmeans/iter", "/tmp/kmeans/cluster");
		KMeansIter.runJob("/tmp/kmeans/cluster", "/tmp/kmeans/data", "/tmp/kmeans/center", "/tmp/kmeans/iter", "/tmp/kmeans/cluster");
	}

}
