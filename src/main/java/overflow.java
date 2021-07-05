import java.util.Vector;

public class overflow extends pages {
	String tableName;
	Object min;
	Object max;
	int count;
	int numOverflow;
	transient  Vector<Tuples>tuples;
	
	
	public overflow(String tableName) {
		super(tableName);
	}

}
