import java.io.Serializable;
import java.util.Arrays;

public class pair implements Serializable,Comparable <pair>{
	    Object min ;
	    Object max ;
	  
	    public pair( Object l, Object r){
	        this.min = l;
	        this.max = r;
	    }
	    public static void main(String[] args) {
//			System.out.println("gego".compareTo("dareen"));
//			Object min ="gego";
//			Object t= "dareen";
//			Object max ="louji";
//			
//			if((t.toString()).compareTo((min.toString()))<0)
//				min=t;
//			if((t.toString()).compareTo((max.toString()))>0)
//				max=t;
//			
//			System.out.println(min.toString());
//			System.out.println(max.toString());
	    	
	    	pair []p=new pair[2];
	    	Arrays.fill(p, new pair(null,null));
	    	Object h=p[0].max;
	    	System.out.println(h);
			
			
		}
		@Override
		public int compareTo(pair o) {
			return ( (this.min).toString()).compareTo((o.min).toString() );
			
		}
}
