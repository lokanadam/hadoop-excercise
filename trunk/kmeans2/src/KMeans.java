public class KMeans{

	public static void main(String args[]) throws Exception{
		DataPrepare.runJob(args[0], "/kmeans/dataprepare");
		Canopy.runJob("/kmeans/dataprepare","/kmeans/canopy", "100");
		MarkData.runJob("/kmeans/dataprepare", "/kmeans/canopy","/kmeans/data");
		KMeansIter.runJob("/kmeans/data", "/kmeans/canopy" , "/kmeans/iter", 1 );
		Viewer.runJob("/kmeans/iter1", "/kmeans/cluster");
	}

}
