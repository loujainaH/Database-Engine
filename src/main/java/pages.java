import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;


public class pages extends Vector{
	String tableName;
	Object min;
	Object max;
	int count;
	int numOverflow;
	transient  Vector<Tuples>tuples;
	transient Vector overflow ;
	ObjectOutputStream outp;

	public pages(String tableName) {
		super();
		this.tableName=tableName;
		this.tuples=new Vector<Tuples>();
		this.overflow=new Vector<overflow>();
		this.count=0;
		this.numOverflow = 0;
	}
	
	public  boolean insertToPage( boolean lastPage ,int pageNumber , String name ,Hashtable<String, Object> columnNameValue,String pk ,ArrayList<String> tableInput) throws ParseException, FileNotFoundException, IOException, ClassNotFoundException {
		//System.out.println("BEFORE READING THE OVERFLOW PAGES VECTOR ============================ "+this.overflow.size());
		this.overflow = readOverflowVector(tableName);
		//System.out.println("WHile READING THE OVERFLOW PAGES VECTOR ============================ "+(readOverflowVector(tableName)).size());
		//System.out.println("After READING THE OVERFLOW PAGES VECTOR ============================ "+this.overflow.size());

		Tuples t = new Tuples( columnNameValue,pk,tableInput);
		Object p= getCkValue(columnNameValue,pk);
		String type= chkType(p);
		this.tuples=readPage(name,pageNumber);
		int[]config =DBApp.getconfig();
	    int n = config[0];
			
		switch(type) {
		case"java.lang.Integer":
			if((this.min== null ) || (int) (t.clusterKey) < ( (int) this.min ) ) {
				this.min=t.clusterKey;
			}
			if(  (this.max== null ) || ((int) (t.clusterKey) > (int)this.max)) {
				this.max=t.clusterKey;
			}
			if(lastPage && ( this.tuples.size() >=n)) {
				System.out.println("GOWA NEW PART FEL INSERTTOPAGE");
				return true;
			}
			else {
		    if(tuples.size() >= n ) {
		    	if( overflow.size() == 0) {
		    		System.out.println("1ST OVERFLOW PAGE!!!!!");
		    		 this.overflow=new Vector<overflow>();
		    		 overflow overflowPage= new overflow(tableName) ;
		    		 overflowPage.tuples = new Vector<Tuples>();
		    		 overflow.add(overflowPage);
		    		 numOverflow= numOverflow+1;
		    		 overflowPage.count=overflowPage.count+1;
		    		 (overflowPage.tuples).add(t);
		    		 overflowPage.min=overflowPage.tuples.get(0).clusterKey;
		    		 overflowPage.max=overflowPage.tuples.get((overflowPage.tuples.size()-1)).clusterKey;
		    		 name = name +"_"+pageNumber +"_overflow";
		    		 writeTupleToPage( name , 0 , overflowPage.tuples );
		    		
		    	}
		    	else {
		    		boolean inserted = false;
		    		for(int j=0 ; j< overflow.size() ; j++) {
		    			overflow x = (overflow) overflow.get(j);
		    			String xName = name +"_"+pageNumber +"_overflow";
		    			x.tuples=readPage( xName ,j);
		    			if (x.count < n) {
			    				x.count  =  x.count+1;
			    				if(x.tuples == null ) {
			    					x.tuples = new Vector<Tuples>();
			    				}
			    				(x.tuples).add(t);
			    			    Collections.sort(x.tuples);
			    			    inserted = true;
			    			    x.min=x.tuples.get(0).clusterKey;
			   	    		 	x.max=x.tuples.get((x.tuples.size()-1)).clusterKey;
				   	    		 name = name+"_"+pageNumber +"_overflow";
					    		 writeTupleToPage( name , j , x.tuples );
			   	    		 	break;
		    			}
		    		}
		    		if (!inserted) {
		    			overflow overflowPage= new overflow(tableName) ;
		    			overflowPage.count=overflowPage.count + 1;
			    		overflowPage.tuples = new Vector<Tuples>();
			    		overflow.add(overflowPage);
			    		numOverflow= numOverflow+1;
			    		(overflowPage.tuples).add(t);
			    		 overflowPage.min=overflowPage.tuples.get(0).clusterKey;
			    		 overflowPage.max=overflowPage.tuples.get((overflowPage.tuples.size()-1)).clusterKey;
			    		 name = name +"_"+pageNumber +"_overflow";
			    		 writeTupleToPage( name , numOverflow , overflowPage.tuples );
		    		}
		    	}
		    }
		    else {
		    	this.count = count+1;
		    	tuples.add(t);
			    Collections.sort(tuples);
				writeTupleToPage( name , pageNumber , tuples );
			 }
		    }
		    writeOverflowVector(tableName , this.overflow);
		    return false;
		case"java.lang.Double":
			
			if((this.min== null ) || (Double) (t.clusterKey) < ( (Double) this.min ) ) {
				this.min=t.clusterKey;
			}
			if(  (this.max== null ) || ((Double) (t.clusterKey) > (Double)this.max)) {
				this.max=t.clusterKey;
			}
			if(lastPage && ( this.tuples.size() >=n)) {
				System.out.println("GOWA NEW PART FEL INSERTTOPAGE");
				return true;
			}
			else {
		    if(tuples.size() >= n ) {
		    	if( overflow.size() == 0) {
		    		
		    		 this.overflow=new Vector<overflow>();
		    		 overflow overflowPage= new overflow(tableName) ;
		    		 overflowPage.tuples = new Vector<Tuples>();
		    		 overflow.add(overflowPage);
		    		 numOverflow= numOverflow+1;
		    		 overflowPage.count=overflowPage.count+1;
		    		 (overflowPage.tuples).add(t);
		    		 overflowPage.min=overflowPage.tuples.get(0).clusterKey;
		    		 overflowPage.max=overflowPage.tuples.get((overflowPage.tuples.size()-1)).clusterKey;
		    		 name = name +"_"+pageNumber +"_overflow";
		    		 writeTupleToPage( name , 0 , overflowPage.tuples );
		    		
		    	}
		    	else {
		    		boolean inserted = false;
		    		for(int j=0 ; j< overflow.size() ; j++) {
		    			overflow x = (overflow) overflow.get(j);
		    			String xName = name +"_"+pageNumber +"_overflow";
		    			x.tuples=readPage( xName ,j);
		    			if (x.count < n) {
			    				x.count  =  x.count+1;
			    				if(x.tuples == null ) {
			    					x.tuples = new Vector<Tuples>();
			    				}
			    				(x.tuples).add(t);
			    			    Collections.sort(x.tuples);
			    			    inserted = true;
			    			    x.min=x.tuples.get(0).clusterKey;
			   	    		 	x.max=x.tuples.get((x.tuples.size()-1)).clusterKey;
				   	    		 name = name+"_"+pageNumber +"_overflow";
					    		 writeTupleToPage( name , j , x.tuples );
			   	    		 	break;
		    			}
		    		}
		    		if (!inserted) {
		    			overflow overflowPage= new overflow(tableName) ;
		    			overflowPage.count=overflowPage.count + 1;
			    		overflowPage.tuples = new Vector<Tuples>();
			    		overflow.add(overflowPage);
			    		numOverflow= numOverflow+1;
			    		(overflowPage.tuples).add(t);
			    		 overflowPage.min=overflowPage.tuples.get(0).clusterKey;
			    		 overflowPage.max=overflowPage.tuples.get((overflowPage.tuples.size()-1)).clusterKey;
			    		 name = name +"_"+pageNumber +"_overflow";
			    		 writeTupleToPage( name , numOverflow , overflowPage.tuples );
		    		}
		    	}
		    }
		    else {
		    	this.count = count+1;
		    	tuples.add(t);
			    Collections.sort(tuples);
				writeTupleToPage( name , pageNumber , tuples );
			 }
		    }
		    writeOverflowVector(tableName , this.overflow);
		    return false;
		case"java.lang.String":
			
			if((this.min== null ) || ((String)(this.min)).compareTo((String)t.clusterKey)>0) {  
			this.min=t.clusterKey;
		
			}
		if(  (this.max== null ) || ((String)(this.max)).compareTo((String)t.clusterKey)<0) {
			this.max=t.clusterKey;
		}
		if(lastPage && ( this.tuples.size() >=n)) {
			//System.out.println("GOWA NEW PART FEL INSERTTOPAGE");
			return true;
		}
		else {
	    if(tuples.size() >= n ) {
	    	if( overflow.size() == 0) {
	    		//System.out.println("1ST OVERFLOW PAGEEEEEEEEEEEEEE");

	    		 this.overflow=new Vector<overflow>();
	    		 overflow overflowPage= new overflow(tableName) ;
	    		 overflowPage.tuples = new Vector<Tuples>();
	    		 overflow.add(overflowPage);
	    		 numOverflow= numOverflow+1;
	    		 overflowPage.count=overflowPage.count+1;
	    		 (overflowPage.tuples).add(t);
	    		 overflowPage.min=overflowPage.tuples.get(0).clusterKey;
	    		 overflowPage.max=overflowPage.tuples.get((overflowPage.tuples.size()-1)).clusterKey;
	    		 name = name +"_"+pageNumber +"_overflow";
	    		 writeTupleToPage( name , 0 , overflowPage.tuples );
	    		
	    	}
	    	else {
	    		boolean inserted = false;
	    		for(int j=0 ; j< overflow.size(); j++) {
	    			overflow x = (overflow) overflow.get(j);
	    			String xName = name +"_"+pageNumber +"_overflow";
	    			x.tuples=readPage( xName ,j);
	    			if (x.count < n) {
			    		//System.out.println("FE MAKAN FEL OVERFLOW============================");	
	    				x.count  =  x.count+1;
		    				if(x.tuples == null ) {
		    					x.tuples = new Vector<Tuples>();
		    				}
		    	    		(x.tuples).add(t);
		    			    Collections.sort(x.tuples);
		    			    inserted = true;
		    			    x.min=x.tuples.get(0).clusterKey;
		   	    		 	x.max=x.tuples.get((x.tuples.size()-1)).clusterKey;
			   	    		 name = name+"_"+pageNumber +"_overflow";
				    		 writeTupleToPage( name , j , x.tuples );
		   	    		 	break;
	    			}
	    		}
	    		if (!inserted) {
	    			overflow overflowPage= new overflow(tableName) ;
	    			overflowPage.count=overflowPage.count + 1;
		    		overflowPage.tuples = new Vector<Tuples>();
		    		overflow.add(overflowPage);
		    		//System.out.println("NEW OVERFLOW BUT NOT THE 1ST ONE");
		    		numOverflow= numOverflow+1;
		    		(overflowPage.tuples).add(t);
		    		 overflowPage.min=overflowPage.tuples.get(0).clusterKey;
		    		 overflowPage.max=overflowPage.tuples.get((overflowPage.tuples.size()-1)).clusterKey;
		    		 name = name +"_"+pageNumber +"_overflow";
		    		 writeTupleToPage( name , numOverflow , overflowPage.tuples );
	    		}
	    	}
	    }
	    else {
	    	this.count = count+1;
	    	tuples.add(t);
		    Collections.sort(tuples);
			writeTupleToPage( name , pageNumber , tuples );
		 }
	    }
	    writeOverflowVector(tableName , this.overflow);
	    return false;
	    
		case"java.util.Date":
			if((this.min== null ) || ((Date)(this.min)).compareTo((Date)t.clusterKey)>0) {  
				this.min=t.clusterKey;
			
				}
			if(  (this.max== null ) || ((Date)(this.max)).compareTo((Date)t.clusterKey)<0) {
				this.max=t.clusterKey;
			}
			if(lastPage && ( this.tuples.size() >=n)) {
				System.out.println("GOWA NEW PART FEL INSERTTOPAGE");
				return true;
			}
			else {
		    if(tuples.size() >= n ) {
		    	if( overflow.size() == 0) {
		    		
		    		 this.overflow=new Vector<overflow>();
		    		 overflow overflowPage= new overflow(tableName) ;
		    		 overflowPage.tuples = new Vector<Tuples>();
		    		 overflow.add(overflowPage);
		    		 numOverflow= numOverflow+1;
		    		 overflowPage.count=overflowPage.count+1;
		    		 (overflowPage.tuples).add(t);
		    		 overflowPage.min=overflowPage.tuples.get(0).clusterKey;
		    		 overflowPage.max=overflowPage.tuples.get((overflowPage.tuples.size()-1)).clusterKey;
		    		 name = name +"_"+pageNumber +"_overflow";
		    		 writeTupleToPage( name , 0 , overflowPage.tuples );
		    		
		    	}
		    	else {
		    		boolean inserted = false;
		    		for(int j=0 ; j< overflow.size() ; j++) {
		    			overflow x = (overflow) overflow.get(j);
		    			String xName = name +"_"+pageNumber +"_overflow";
		    			x.tuples=readPage( xName ,j);
		    			if (x.count < n) {
			    				x.count  =  x.count+1;
			    				if(x.tuples == null ) {
			    					x.tuples = new Vector<Tuples>();
			    				}
			    				(x.tuples).add(t);
			    			    Collections.sort(x.tuples);
			    			    inserted = true;
			    			    x.min=x.tuples.get(0).clusterKey;
			   	    		 	x.max=x.tuples.get((x.tuples.size()-1)).clusterKey;
				   	    		 name = name+"_"+pageNumber +"_overflow";
					    		 writeTupleToPage( name , j , x.tuples );
			   	    		 	break;
		    			}
		    		}
		    		if (!inserted) {
		    			overflow overflowPage= new overflow(tableName) ;
		    			overflowPage.count=overflowPage.count + 1;
			    		overflowPage.tuples = new Vector<Tuples>();
			    		overflow.add(overflowPage);
			    		numOverflow= numOverflow+1;
			    		(overflowPage.tuples).add(t);
			    		 overflowPage.min=overflowPage.tuples.get(0).clusterKey;
			    		 overflowPage.max=overflowPage.tuples.get((overflowPage.tuples.size()-1)).clusterKey;
			    		 name = name +"_"+pageNumber +"_overflow";
			    		 writeTupleToPage( name , numOverflow , overflowPage.tuples );
		    }}}
		    else {
		    	this.count = count+1;
		    	tuples.add(t);
			    Collections.sort(tuples);
				writeTupleToPage( name , pageNumber , tuples );
	
			 }
		    }
		    writeOverflowVector(tableName , this.overflow);

		default:		   
			writeOverflowVector(tableName , this.overflow);
			return false;
		}
	}
	
	 public  Vector<Grid> readGrids (String Name) throws IOException, ClassNotFoundException {
			String fileName = "src/main/resources/data/" + Name+ ".grids";
			 	Vector<Grid>tt = new Vector<Grid>();
		        File file = new File(fileName);
		        if (file.exists()) {
		        	FileInputStream fileStream = new FileInputStream(fileName);
		     	     ObjectInputStream   is = new ObjectInputStream(fileStream);
		     	     tt = (Vector<Grid>) is.readObject();
		            return tt;
		       }
	        return tt; 
		}

	public  boolean insertToPageGrid( boolean lastPage ,int pageNumber , String name ,Hashtable<String, Object> columnNameValue,String pk ,ArrayList<String> tableInput) throws ParseException, FileNotFoundException, IOException, ClassNotFoundException {
	    System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!AWEL MA BNED%OL INSERTTOPAGE!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
		Vector<Grid>grids=readGrids(name);
		String pageName=tableName+"_"+pageNumber;
		
		this.overflow = readOverflowVector(tableName);
		Tuples t = new Tuples( columnNameValue,pk,tableInput);
		Object p= getCkValue(columnNameValue,pk);
		String type= chkType(p);
		this.tuples=readPage(name,pageNumber);
		int[]config =DBApp.getconfig();
	    int n = config[0];
		count=this.tuples.size();	
		this.numOverflow=this.overflow.size();
		switch(type) {
		
		case"java.lang.Integer":
			if((this.min== null ) || (int) (t.clusterKey) < ( (int) this.min ) ) {
				this.min=t.clusterKey;
			}
			if(  (this.max== null ) || ((int) (t.clusterKey) > (int)this.max)) {
				this.max=t.clusterKey;
			}
//			if(lastPage && ( this.tuples.size() >=n)) {
//				System.out.println("GOWA NEW PART FEL INSERTTOPAGE");
//				return true;
//			}
			 {//hena kan fe else
				System.out.println("the value of n :      "+n);
				System.out.println("tuples.size()        :"+tuples.size());
		    if(tuples.size() >= n ) {
		    	if( overflow.size() == 0) {
		    		System.out.println("1ST OVERFLOW PAGE!!!!!");
		    		 this.overflow=new Vector<overflow>();
		    		 overflow overflowPage= new overflow(tableName) ;
		    		 overflowPage.tuples = new Vector<Tuples>();
		    		 overflow.add(overflowPage);
		    		 numOverflow= numOverflow+1;
		    		 overflowPage.count=overflowPage.count+1;
		    		 (overflowPage.tuples).add(t);
		    		 overflowPage.min=overflowPage.tuples.get(0).clusterKey;
		    		 overflowPage.max=overflowPage.tuples.get((overflowPage.tuples.size()-1)).clusterKey;
		    		 name = name +"_"+pageNumber +"_overflow";
		    		 writeTupleToPage( name , 0 , overflowPage.tuples );
		    		 if (grids.size()!=0) {
		    			 grids.get(0).insertInBucket(pageName+"_overflow_0", t, tableName, grids);
		    		 }
		    		 
		    		
		    	}
		    	else {
		    		boolean inserted = false;
		    		for(int j=0 ; j< overflow.size() ; j++) {
		    			overflow x = (overflow) overflow.get(j);
		    			String xName = name +"_"+pageNumber +"_overflow";
		    			x.tuples=readPage( xName ,j);
		    			if (x.count < n) {
			    				x.count  =  x.count+1;
			    				if(x.tuples == null ) {
			    					x.tuples = new Vector<Tuples>();
			    				}
			    				(x.tuples).add(t);
			    			    Collections.sort(x.tuples);
			    			    inserted = true;
			    			    x.min=x.tuples.get(0).clusterKey;
			   	    		 	x.max=x.tuples.get((x.tuples.size()-1)).clusterKey;
				   	    		 name = name+"_"+pageNumber +"_overflow";
					    		 writeTupleToPage( name , j , x.tuples );
					    		 if (grids.size()!=0) {
					    			 grids.get(0).insertInBucket(pageName+"_overflow_"+j, t, tableName, grids);
					    		 }
			   	    		 	break;
		    			}
		    		}
		    		if (!inserted) {
		    			overflow overflowPage= new overflow(tableName) ;
		    			overflowPage.count=overflowPage.count + 1;
			    		overflowPage.tuples = new Vector<Tuples>();
			    		overflow.add(overflowPage);
			    		numOverflow= numOverflow+1;
			    		(overflowPage.tuples).add(t);
			    		 overflowPage.min=overflowPage.tuples.get(0).clusterKey;
			    		 overflowPage.max=overflowPage.tuples.get((overflowPage.tuples.size()-1)).clusterKey;
			    		 name = name +"_"+pageNumber +"_overflow";
			    		 writeTupleToPage( name , numOverflow , overflowPage.tuples );
			    		 if (grids.size()!=0) {
			    			 grids.get(0).insertInBucket(pageName+"_overflow_"+numOverflow, t, tableName, grids);
			    		 }
		    		}
		    	}
		    }
		    else {
		    	System.out.println("------------last else---------------");
		    	this.count = count+1;
		    	tuples.add(t);
			    Collections.sort(tuples);
				writeTupleToPage( name , pageNumber , tuples );
				if (grids.size()!=0) {
	    			 grids.get(0).insertInBucket(pageName, t, tableName, grids);
	    		 }
			 }
		    }
		    writeOverflowVector(tableName , this.overflow);
		    return false;
		case"java.lang.Double":
			
			if((this.min== null ) || (Double) (t.clusterKey) < ( (Double) this.min ) ) {
				this.min=t.clusterKey;
			}
			if(  (this.max== null ) || ((Double) (t.clusterKey) > (Double)this.max)) {
				this.max=t.clusterKey;
			}
//			if(lastPage && ( this.tuples.size() >=n)) {
//				System.out.println("GOWA NEW PART FEL INSERTTOPAGE");
//				return true;
//			}
			 {
		    if(tuples.size() >= n ) {
		    	if( overflow.size() == 0) {
		    		
		    		 this.overflow=new Vector<overflow>();
		    		 overflow overflowPage= new overflow(tableName) ;
		    		 overflowPage.tuples = new Vector<Tuples>();
		    		 overflow.add(overflowPage);
		    		 numOverflow= numOverflow+1;
		    		 overflowPage.count=overflowPage.count+1;
		    		 (overflowPage.tuples).add(t);
		    		 overflowPage.min=overflowPage.tuples.get(0).clusterKey;
		    		 overflowPage.max=overflowPage.tuples.get((overflowPage.tuples.size()-1)).clusterKey;
		    		 name = name +"_"+pageNumber +"_overflow";
		    		 writeTupleToPage( name , 0 , overflowPage.tuples );
		    		 if (grids.size()!=0) {
		    			 grids.get(0).insertInBucket(pageName+"_overflow_0", t, tableName, grids);
		    		 }
		    		
		    	}
		    	else {
		    		boolean inserted = false;
		    		for(int j=0 ; j< overflow.size() ; j++) {
		    			overflow x = (overflow) overflow.get(j);
		    			String xName = name +"_"+pageNumber +"_overflow";
		    			x.tuples=readPage( xName ,j);
		    			if (x.count < n) {
			    				x.count  =  x.count+1;
			    				if(x.tuples == null ) {
			    					x.tuples = new Vector<Tuples>();
			    				}
			    				(x.tuples).add(t);
			    			    Collections.sort(x.tuples);
			    			    inserted = true;
			    			    x.min=x.tuples.get(0).clusterKey;
			   	    		 	x.max=x.tuples.get((x.tuples.size()-1)).clusterKey;
				   	    		 name = name+"_"+pageNumber +"_overflow";
					    		 writeTupleToPage( name , j , x.tuples );
					    		 if (grids.size()!=0) {
					    			 grids.get(0).insertInBucket(pageName+"_overflow_"+j, t, tableName, grids);
					    		 }
			   	    		 	break;
		    			}
		    		}
		    		if (!inserted) {
		    			overflow overflowPage= new overflow(tableName) ;
		    			overflowPage.count=overflowPage.count + 1;
			    		overflowPage.tuples = new Vector<Tuples>();
			    		overflow.add(overflowPage);
			    		numOverflow= numOverflow+1;
			    		(overflowPage.tuples).add(t);
			    		 overflowPage.min=overflowPage.tuples.get(0).clusterKey;
			    		 overflowPage.max=overflowPage.tuples.get((overflowPage.tuples.size()-1)).clusterKey;
			    		 name = name +"_"+pageNumber +"_overflow";
			    		 writeTupleToPage( name , numOverflow , overflowPage.tuples );
			    		 if (grids.size()!=0) {
			    			 grids.get(0).insertInBucket(pageName+"_overflow_"+numOverflow, t, tableName, grids);
			    		 }
		    		}
		    	}
		    }
		    else {
		    	this.count = count+1;
		    	tuples.add(t);
			    Collections.sort(tuples);
				writeTupleToPage( name , pageNumber , tuples );
				if (grids.size()!=0) {
	    			 grids.get(0).insertInBucket(pageName, t, tableName, grids);
	    		 }
			 }
		    }
		    writeOverflowVector(tableName , this.overflow);
		    return false;
		case"java.lang.String":
			
			if((this.min== null ) || ((String)(this.min)).compareTo((String)t.clusterKey)>0) {  
			this.min=t.clusterKey;
		
			}
		if(  (this.max== null ) || ((String)(this.max)).compareTo((String)t.clusterKey)<0) {
			this.max=t.clusterKey;
		}
//		if(lastPage && ( this.tuples.size() >=n)) {
//			//System.out.println("GOWA NEW PART FEL INSERTTOPAGE");
//			return true;
//		}
		 {
	    if(tuples.size() >= n ) {
	    	if( overflow.size() == 0) {
	    		//System.out.println("1ST OVERFLOW PAGEEEEEEEEEEEEEE");

	    		 this.overflow=new Vector<overflow>();
	    		 overflow overflowPage= new overflow(tableName) ;
	    		 overflowPage.tuples = new Vector<Tuples>();
	    		 overflow.add(overflowPage);
	    		 numOverflow= numOverflow+1;
	    		 overflowPage.count=overflowPage.count+1;
	    		 (overflowPage.tuples).add(t);
	    		 overflowPage.min=overflowPage.tuples.get(0).clusterKey;
	    		 overflowPage.max=overflowPage.tuples.get((overflowPage.tuples.size()-1)).clusterKey;
	    		 name = name +"_"+pageNumber +"_overflow";
	    		 writeTupleToPage( name , 0 , overflowPage.tuples );
	    		 if (grids.size()!=0) {
	    			 grids.get(0).insertInBucket(pageName+"_overflow_0", t, tableName, grids);
	    		 }
	    		
	    	}
	    	else {
	    		boolean inserted = false;
	    		for(int j=0 ; j< overflow.size(); j++) {
	    			overflow x = (overflow) overflow.get(j);
	    			String xName = name +"_"+pageNumber +"_overflow";
	    			x.tuples=readPage( xName ,j);
	    			if (x.count < n) {
			    		//System.out.println("FE MAKAN FEL OVERFLOW============================");	
	    				x.count  =  x.count+1;
		    				if(x.tuples == null ) {
		    					x.tuples = new Vector<Tuples>();
		    				}
		    	    		(x.tuples).add(t);
		    			    Collections.sort(x.tuples);
		    			    inserted = true;
		    			    x.min=x.tuples.get(0).clusterKey;
		   	    		 	x.max=x.tuples.get((x.tuples.size()-1)).clusterKey;
			   	    		 name = name+"_"+pageNumber +"_overflow";
				    		 writeTupleToPage( name , j , x.tuples );
				    		 if (grids.size()!=0) {
				    			 grids.get(0).insertInBucket(pageName+"_overflow_"+j, t, tableName, grids);
				    		 }
		   	    		 	break;
	    			}
	    		}
	    		if (!inserted) {
	    			overflow overflowPage= new overflow(tableName) ;
	    			overflowPage.count=overflowPage.count + 1;
		    		overflowPage.tuples = new Vector<Tuples>();
		    		overflow.add(overflowPage);
		    		//System.out.println("NEW OVERFLOW BUT NOT THE 1ST ONE");
		    		numOverflow= numOverflow+1;
		    		(overflowPage.tuples).add(t);
		    		 overflowPage.min=overflowPage.tuples.get(0).clusterKey;
		    		 overflowPage.max=overflowPage.tuples.get((overflowPage.tuples.size()-1)).clusterKey;
		    		 name = name +"_"+pageNumber +"_overflow";
		    		 writeTupleToPage( name , numOverflow , overflowPage.tuples );
		    		 if (grids.size()!=0) {
		    			 grids.get(0).insertInBucket(pageName+"_overflow_"+numOverflow, t, tableName, grids);
		    		 }
	    		}
	    	}
	    }
	    else {
	    	this.count = count+1;
	    	tuples.add(t);
		    Collections.sort(tuples);
			writeTupleToPage( name , pageNumber , tuples );
			if (grids.size()!=0) {
   			 grids.get(0).insertInBucket(pageName, t, tableName, grids);
   		 }
		 }
	    }
	    writeOverflowVector(tableName , this.overflow);
	    return false;
	    
		case"java.util.Date":
			if((this.min== null ) || ((Date)(this.min)).compareTo((Date)t.clusterKey)>0) {  
				this.min=t.clusterKey;
			
				}
			if(  (this.max== null ) || ((Date)(this.max)).compareTo((Date)t.clusterKey)<0) {
				this.max=t.clusterKey;
			}
//			if(lastPage && ( this.tuples.size() >=n)) {
//				System.out.println("GOWA NEW PART FEL INSERTTOPAGE");
//				return true;
//			}
			 {
		    if(tuples.size() >= n ) {
		    	if( overflow.size() == 0) {
		    		
		    		 this.overflow=new Vector<overflow>();
		    		 overflow overflowPage= new overflow(tableName) ;
		    		 overflowPage.tuples = new Vector<Tuples>();
		    		 overflow.add(overflowPage);
		    		 numOverflow= numOverflow+1;
		    		 overflowPage.count=overflowPage.count+1;
		    		 (overflowPage.tuples).add(t);
		    		 overflowPage.min=overflowPage.tuples.get(0).clusterKey;
		    		 overflowPage.max=overflowPage.tuples.get((overflowPage.tuples.size()-1)).clusterKey;
		    		 name = name +"_"+pageNumber +"_overflow";
		    		 writeTupleToPage( name , 0 , overflowPage.tuples );
		    		 if (grids.size()!=0) {
		    			 grids.get(0).insertInBucket(pageName+"_overflow_0", t, tableName, grids);
		    		 }
		    		
		    	}
		    	else {
		    		boolean inserted = false;
		    		for(int j=0 ; j< overflow.size() ; j++) {
		    			overflow x = (overflow) overflow.get(j);
		    			String xName = name +"_"+pageNumber +"_overflow";
		    			x.tuples=readPage( xName ,j);
		    			if (x.count < n) {
			    				x.count  =  x.count+1;
			    				if(x.tuples == null ) {
			    					x.tuples = new Vector<Tuples>();
			    				}
			    				(x.tuples).add(t);
			    			    Collections.sort(x.tuples);
			    			    inserted = true;
			    			    x.min=x.tuples.get(0).clusterKey;
			   	    		 	x.max=x.tuples.get((x.tuples.size()-1)).clusterKey;
				   	    		 name = name+"_"+pageNumber +"_overflow";
					    		 writeTupleToPage( name , j , x.tuples );
					    		 if (grids.size()!=0) {
					    			 grids.get(0).insertInBucket(pageName+"_overflow_"+j, t, tableName, grids);
					    		 }
			   	    		 	break;
		    			}
		    		}
		    		if (!inserted) {
		    			overflow overflowPage= new overflow(tableName) ;
		    			overflowPage.count=overflowPage.count + 1;
			    		overflowPage.tuples = new Vector<Tuples>();
			    		overflow.add(overflowPage);
			    		numOverflow= numOverflow+1;
			    		(overflowPage.tuples).add(t);
			    		 overflowPage.min=overflowPage.tuples.get(0).clusterKey;
			    		 overflowPage.max=overflowPage.tuples.get((overflowPage.tuples.size()-1)).clusterKey;
			    		 name = name +"_"+pageNumber +"_overflow";
			    		 writeTupleToPage( name , numOverflow , overflowPage.tuples );
			    		 if (grids.size()!=0) {
			    			 grids.get(0).insertInBucket(pageName+"_overflow_"+numOverflow, t, tableName, grids);
			    		 }
		    }}}
		    else {
		    	this.count = count+1;
		    	tuples.add(t);
			    Collections.sort(tuples);
				writeTupleToPage( name , pageNumber , tuples );
				if (grids.size()!=0) {
	    			 grids.get(0).insertInBucket(pageName, t, tableName, grids);
	    		 }
	
			 }
		    }
		    writeOverflowVector(tableName , this.overflow);

		default:		   
			writeOverflowVector(tableName , this.overflow);
			return false;
		}
	}
	
	
	
	
	public  Object getCkValue(Hashtable <String,Object> input , String pk ) {
	        ArrayList<Object> tuplesKey = new ArrayList();
	
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
	            if(key.equals(pk)) {
	                    return (tuplesKey.get(i+1));
	             }
	        }
	        return -1;
	    }
	
	public  String chkType(Object var) throws ParseException{
        String type = var.getClass().toString();
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
	
	public  boolean insertToPage2( boolean lastPage ,int pageNumber , String name ,Hashtable<String, Object> columnNameValue,String pk ,ArrayList<String> tableInput) throws ParseException, FileNotFoundException, IOException, ClassNotFoundException {
		Tuples t = new Tuples( columnNameValue,pk,tableInput);
		
		if((this.min== null ) || (int) (t.clusterKey) < ( (int) this.min ) ) {
			this.min=t.clusterKey;
		}
		if(  (this.max== null ) || ((int) (t.clusterKey) > (int)this.max)) {
			this.max=t.clusterKey;
		}
		
		
	    this.tuples=readPage(name,pageNumber);
	    int[]config =DBApp.getconfig();
		int n = config[0];
		
		if(lastPage && ( this.tuples.size() >=n)) {
			System.out.println("GOWA NEW PART FEL INSERTTOPAGE");
			return true;
		}
		return false;
	}
	
	public  boolean isFull() {
		int[]config =DBApp.getconfig();
		int n = config[0];
		if (tuples.size()>=n) {
		    return true;
		    }
		else {
			return false;
			}
	}
	
	public  void writeOverflowVector(String tableName, Vector  t) throws FileNotFoundException, IOException {
		String fileName = "src/main/resources/data/" + tableName+"_overflow.pages";
	    try {
	    	ObjectOutputStream os =new ObjectOutputStream(new FileOutputStream(fileName) );
	    	os.writeObject(t);
	    	os.close();	
		} catch (Exception e) {
			// TODO: handle exception
		}    
  }
	
	public  Vector readOverflowVector (String tableName) throws IOException, ClassNotFoundException {
		String fileName = "src/main/resources/data/" + tableName+"_overflow.pages";
		Vector tt = new Vector<overflow>();
        File file = new File(fileName);
        if (file.exists()) {
        	 FileInputStream fileStream = new FileInputStream(fileName);
     	     ObjectInputStream   is = new ObjectInputStream(fileStream);
     	     tt = (Vector) is.readObject();
     	     return tt ;
     	     }  
        return tt; 
}
	
	
	
	public  void writeTupleToPage(String tableName , int i , Vector  t) throws FileNotFoundException, IOException {
		String fileName = "src/main/resources/data/" + tableName+"_"+ i + ".pages";
	    try {
	    	ObjectOutputStream os =new ObjectOutputStream(new FileOutputStream(fileName) );
	    	os.writeObject(t);
	    	os.close();	
		} catch (Exception e) {
			// TODO: handle exception
		}    
  }
	
	public  Vector readPage (String tableName, int pageN) throws IOException, ClassNotFoundException {
			String fileName = "src/main/resources/data/"+tableName+"_"+pageN+".pages";
		 	Vector<Tuples>tt = new Vector<Tuples>();
	        File file = new File(fileName);
	        if (file.exists()) {
	        	FileInputStream fileStream = new FileInputStream(fileName);
	     	     ObjectInputStream   is = new ObjectInputStream(fileStream);
	     	     tt = (Vector) is.readObject();
	            return tt;
	       }
        return tt; 
	}
	
	
	public static void main (String[]args) throws ParseException, FileNotFoundException, IOException, ClassNotFoundException {
		// Hashtable<String, Object> row1 = new Hashtable();
//	        row1.put("id", new Integer( 78452 )); 
//	        row1.put("name", new String("Zaky Noor" ) ); 
//	        row1.put("gpa", new Double( 0.88 ) );
//	        ArrayList<String> t=new ArrayList();
//	        t.add("id");
//	        t.add("gpa");
//	        t.add("name");
//	        Hashtable<String, Object> row2 = new Hashtable();
//	        row2.put("id", new Integer( 18452 )); 
//	        row2.put("name", new String("Loshina" ) ); 
//	        row2.put("gpa", new Double( 0.7 ) );
//	        
//	        Hashtable<String, Object> row3 = new Hashtable();
//	        row3.put("id", new Integer( 8452 )); 
//	        row3.put("name", new String("DDareen" ) ); 
//	        row3.put("gpa", new Double( 0.7 ) );
//	        
//	        Hashtable<String, Object> row4 = new Hashtable();
//	        row4.put("id", new Integer( 98452 )); 
//	        row4.put("name", new String("xingo" ) ); 
//	        row4.put("gpa", new Double( 4.0 ) );
//	        
//	        Hashtable<String, Object> row5 = new Hashtable();
//	        row5.put("id", new Integer( 3 )); 
//	        row5.put("name", new String("Gamden w mafesh el e7na" ) ); 
//	        row5.put("gpa", new Double( 0.0 ) );
		//pages p = new pages("Loji");
//		p.insertToPage(0,"Loji",row1,"name",t);
//		//System.out.println("row1");
//		p.insertToPage(0,"Loji",row2,"name",t);
//		//System.out.println("row2");
//		p.insertToPage(0,"Loji",row3,"name",t);
//		//System.out.println("row3");
//		p.insertToPage(0,"Loji",row4,"name",t);
//		//System.out.println("row4");
		//p.insertToPage(0,"Loji",row5,"name",t);
//		//System.out.println("row5");
//		
//       Vector tt= readPage("Loji",0);
//      
//	    for (int i =0 ; i< tt.size();i++) {
//	    	System.out.println(((Tuples)(tt.get(i))).clusterKey);
//	    }
//	
	
	}
	

}

