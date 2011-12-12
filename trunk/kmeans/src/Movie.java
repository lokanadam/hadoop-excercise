import java.util.ArrayList;
import java.util.Iterator;

public class Movie {

	public final static String MIDSPLITER = ":";
	public final static String U_RSPLITER = ";";
	public final static String UIDSPLITER = ",";
	public final static String CANOPYPLITER = "'";

	public String id;
	public ArrayList<Rating> users;

	public Movie(String id, String data, boolean uidOnly) {
		this.id = id;
		String[] usersWithRating = data.split(U_RSPLITER);

		this.users = new ArrayList<Rating>();

		for (String user : usersWithRating) {
			users.add(new Rating(user, uidOnly));
		}
	}

	public int commonUserCount(Movie movie) {

		Iterator<Rating> callers = this.users.iterator();
		Iterator<Rating> callees = movie.users.iterator();

		int count = 0;
		Integer caller = null, callee = null;

		while (true) {

			if (caller == null) {
				if (callers.hasNext()) {
					caller = callers.next().uid;
				} else {
					break;
				}
			}

			if (callee == null) {
				if (callees.hasNext()) {
					callee = callees.next().uid;
				} else {
					break;
				}
			}

			if (caller.equals(callee)) {
				count++;
				caller = null;
				callee = null;
			} else if (caller.compareTo(callee) < 0) {
				caller = null;
			} else {
				callee = null;
			}
		}

		return count;
	}

	public double cosineDistance(Movie movie) {

		Iterator<Rating> callers = this.users.iterator();
		Iterator<Rating> callees = movie.users.iterator();

		double dotProduct = 0;
		double magnitudeCaller = 0;
		double magnitudeCallee = 0;

		while (callers.hasNext()) {
			magnitudeCaller += callers.next().rating ^ 2;
		}

		while (callees.hasNext()) {
			magnitudeCallee += callees.next().rating ^ 2;
		}

		Rating caller = null, callee = null;

		callers = this.users.iterator();
		callees = movie.users.iterator();

		while (true) {

			if (caller == null) {
				if (callers.hasNext()) {
					caller = callers.next();
				} else {
					break;
				}
			}

			if (callee == null) {
				if (callees.hasNext()) {
					callee = callees.next();
				} else {
					break;
				}
			}

			if (caller.uid.equals(callee.uid)) {
				dotProduct += caller.rating * callee.rating;
				caller = null;
				callee = null;
			} else if (caller.uid.compareTo(callee.uid) < 0) {
				caller = null;
			} else {
				callee = null;
			}
		}

		return (dotProduct * dotProduct)
				/ (1 + magnitudeCaller * magnitudeCallee);
	}

}
