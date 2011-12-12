import java.io.IOException;

public class Driver {

	public final static String STAGE1 = "stage1";
	public final static String STAGE2 = "stage2";
	public final static String STAGE3 = "stage3";
	public final static String STAGE4 = "stage4";

	public static void main(String[] args) throws IOException {

		if (args.length < 2) {
			System.out.println("Usage: Driver <input path> <output path>");
			System.exit(0);
		}

		DataPrep.run(args[0], STAGE1);
		CanopySelection.run(STAGE1, STAGE2);
		MarkData.run(STAGE1, STAGE2, STAGE3);
		KMeans.run(STAGE3, STAGE2, STAGE4, args[1]);
	}
}
