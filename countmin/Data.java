package storm.starter.trident.project.countmin;

public class Data implements Comparable<Data> {
	  public final String message;
	  public final long priority;

	  public Data(String message, long priority) {
	    this.message = message;
	    this.priority = priority;
	  }

	  @Override
	public
	  int compareTo(Data other) {
	    return Long.valueOf(priority).compareTo(other.priority);
	  }

	  // also implement equals() and hashCode()
	}
