public class Rating implements Comparable<Rating> {
	public String user ;
	public double rate ;
	public int count ;
	public Rating(String data){
		user = data.split(":")[0];
		rate = Double.parseDouble(data.split(":")[1]);
		count = 1;
	}

	public void merge(String rate){
		rate += Double.parseDouble(rate);
		count++;
	}

	public int compareTo(Rating other){
		return this.count - other.count;
	}
	public double avg(){
		return rate / count;
	}

}
