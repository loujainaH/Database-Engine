import java.lang.reflect.Array;

public class SQLTerm {
	String _strTableName ;
	String _strColumnName;
	String _strOperator ;
	Object _objValue ;


	public SQLTerm() {
		//this._strTableName = _strTableName;
	}

	public static void main ( String []  args) {
		SQLTerm s = new SQLTerm();
		s._strTableName = "Student";
		s._strColumnName = "name";
		s._strOperator = "="; 
		s._objValue = "John Noor";
		SQLTerm[] arrSQLTerms;
		arrSQLTerms = new SQLTerm[1];
		arrSQLTerms[0] = s;
//		arrSQLTerms[0]._strColumnName= "name";
//		arrSQLTerms[0]._strOperator = "=";
//		arrSQLTerms[0]._objValue = "John Noor";
		System.out.println(arrSQLTerms[0]._strColumnName);
		//System.out.println( false ^ false);
		
		
//		int[] dimensions = { 3 , 3 }; // 2-dimensional array, 3 elements per dimension
//		Object myArray = Array.newInstance(String.class, 3 , 3); // 2D array of strings
//		System.out.println(  ((Array) myArray).get(0,0) );
		
				
		
	}
	
}
