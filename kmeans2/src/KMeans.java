public class KMeans{

	public static void main(String args[]) throws Exception{
		DataPrepare.runJob(args[0], "/kmeans/dataprepare");
		Canopy.runJob("/kmeans/dataprepare","/kmeans/canopy", "100");
		MarkData.runJob("/kmeans/dataprepare", "/kmeans/canopy","/kmeans/data");
		KMeansIter.runJob("/kmeans/canopy", "/kmeans/data", "/kmeans/center", "/kmeans/iter", "/kmeans/cluster");
		KMeansIter.runJob("/kmeans/cluster", "/kmeans/data", "/kmeans/center", "/kmeans/iter", "/kmeans/cluster");
	}

}
