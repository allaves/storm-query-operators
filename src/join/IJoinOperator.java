package join;

import java.sql.ResultSet;
import java.util.Iterator;

public interface IJoinOperator {
	
	public ResultSet execute(Iterable r, Iterable s);

}
