import java.io.Serializable;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

public class Tuples implements Comparable<Tuples> , Serializable {
	Object clusterKey;
	String dataType;
	Object[]tuples;

	
	public Tuples (Hashtable <String,Object>input , String pk ,ArrayList<String> tableInput) throws ParseException {
		ArrayList<Object>tuplesKey= new ArrayList();
		tuples=new Object [tableInput.size()];
   
		Enumeration<String> values1 = input.keys();
	    Enumeration<Object> values2 = input.elements();
	        while(values1.hasMoreElements() ){
	            String data=values1.nextElement().toString();
	            Object data2=values2.nextElement();
	            tuplesKey.add(data);
	            tuplesKey.add(data2);  
	    }
	        for(int i=0;i<tuplesKey.size();i+=2) {
	        	String key= (String) tuplesKey.get(i);
	        	for(int j =0; j<tableInput.size();j++) {
	        		String col= tableInput.get(j);
	        	//	 System.out.println("da el col : "+col);  
	        		if(key.equals(col)) {
	        		 tuples[j]= tuplesKey.get(i+1);
	        		}
	        		//System.out.println("da el pk : "+pk); 
	        		if(key.equals(pk)) {
	        			clusterKey=tuplesKey.get(i+1);
	        		}
	        	}
	        	// System.out.println("da el key : "+key);  
	        }
//	        for (int i =0;i<tuples.length;i++) {
//	        	//System.out.print(tuples[i]+" ");
//	        }
	        
	   //System.out.println(clusterKey);
	    dataType=chkType(clusterKey);     
	}
	
	public  String chkType(Object var) throws ParseException{
	//	System.out.println("in tuples type: "+var);
        String type = var.getClass().toString();
        //System.out.println("in tuples type: "+type);
        String r="";
       if(type.substring(16).equals("Integer")) {
        	r="java.lang.Integer";
        	}
        if(type.substring(16).equals("Double")) {
        	r="java.lang.Double";}
        
        if(type.substring(16).equals("String")) {
    	   r="java.lang.String";
         	}
         if(type.substring(16).equals("Date") ){
             		r="java.util.Date";
	        }
        return r; 
    }
	
	@Override
	public int compareTo(Tuples o) {
		switch(this.dataType) {
		case "java.lang.Integer":
			 return ((Integer) this.clusterKey).compareTo((Integer) o.clusterKey );  
		case "java.lang.Double":
			return ((Double) this.clusterKey).compareTo((Double) o.clusterKey );
		case "java.lang.String":
			return ((String) this.clusterKey).compareTo((String) o.clusterKey );
		case "java.util.Date":
			return ((Date) this.clusterKey).compareTo((Date) o.clusterKey );
		default:return 0;
		}
		
	}

	
	public static void main (String[]args) throws ParseException {
		
		//Date t3 = new Date( 2 , 2 - 1, 2000 - 1900);
		// Integer t3 = 2 ;
		//Double t3 = 0.5 ;
		String t3 = "db";
		String type = t3.getClass().toString();
        //System.out.println("in tuples type: "+type);
        String r="";
        System.out.println((type.substring(16)));
		
//		String table = "students";
//        Hashtable<String, Object> row1 = new Hashtable();
//        row1.put("id", new Integer( 78452 )); 
//        row1.put("name", new String("Zaky Noor" ) ); 
//        row1.put("gpa", new Double( 0.88 ) );
//        ArrayList<String> t=new ArrayList();
//        t.add("id");
//        t.add("gpa");
//        t.add("name");
//        Tuples t1= new Tuples(row1,"id",t);    
//        
//        
//        Hashtable<String, Object> row2 = new Hashtable();
//        row2.put("id", new Integer( 18452 )); 
//        //row2.put("name", new String("gego badrawy" ) ); 
//        row2.put("gpa", new Double( 1.7 ) );
//       
//        
//        Tuples t2= new Tuples(row2,"id",t);
//       // //System.out.println(t2.compareTo(t1));
//        
//       Vector <Tuples> list = new Vector();
//        list.add(t1);
//        list.add(t2);
//       // //System.out.println("Before "+(list.get(0)).clusterKey+" "+list.get(1));
//       // Collections.sort(list);
//        for (int i =0;i<(list.get(1).tuples).length;i++) {
//    	//System.out.print((list.get(1).tuples)[i]+" ");
//    }
//    
//  // //System.out.println("heree   "+"gego".compareTo("dareen"));
//

	}
}
