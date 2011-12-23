public class KMeans{

	public static void main(String args[]) throws Exception{
		//DataPrepare.runJob(args[0], "/kmeans/dataprepare");
		Canopy.runJob("/kmeans/movie1-999","/kmeans/canopy", "1000");
		MarkData.runJob("/kmeans/movie1-999", "/kmeans/canopy","/kmeans/data");
		KMeansIter.runJob("/kmeans/data", "/kmeans/canopy" , "/kmeans/iter", 5 );
		Viewer.runJob("/kmeans/iter5", "/kmeans/cluster");
	}

}
