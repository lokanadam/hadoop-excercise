public class Rating implements Comparable<Rating> {

	public Integer uid = null;

	public int count;
	public int rating;

	public Rating(String data, boolean uidOnly) {

		count = 1;
		String[] tokens = data.split(Movie.UIDSPLITER);

		uid = new Integer(tokens[0]);

		if (!uidOnly) {
			rating = new Integer(tokens[1]);
		}
	}

	public void merge(Rating another) {
		count++;
		rating += another.rating;
	}

	@Override
	public int compareTo(Rating another) {
		return this.count - another.count;
	}
}
