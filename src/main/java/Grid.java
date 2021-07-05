import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;
import java.util.stream.Collectors;

public class Grid implements Serializable{
    Vector <pair[]> grid;
    String[] columnNames;
    int []loc;
    //Vector <buckets> bucketsList;
    
	public Grid(String [] columnNames) {
		this.columnNames=columnNames;
	    this.grid= new Vector<>();
	    this.loc= new int[columnNames.length];
	    
	}
	// table name _ grid number_bucket number 
    public void createGrid(String tableName, String[] columnNames,Table t,int gridN) throws DBAppException, IOException, ClassNotFoundException, ParseException{
		
    	ArrayList<String> tableTag= t.getOrder(tableName);
    	this.loc= new int [columnNames.length];
    	for (int i =0;i<loc.length;i++) {
    		for (int j=0;j<tableTag.size();j++) {
    			if (columnNames[i].equals(tableTag.get(j)))
    				loc[i]=j;
    		}
    	}
    	pair [] range=new pair[columnNames.length];
    	Arrays.fill(range, new pair(null,null));
    	Vector pageList = t.readTablePages(tableName); 
    	String tempo=tableName;
    	
    	
    	
    	BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
		String current = "";
		ArrayList<String> con = new ArrayList<String>();
		while ((current =br.readLine())!= null) {
			String[] line = current.split(",");
			for(int i=0 ;i<line.length;i+=7) {
				if(line[i].equals(tableName)) {
					for (int j=i;j<7;j++) {
						con.add(line[j] );
						}	}	}	}
	
    	System.out.println(grid.size()+"   ");
    	for (int i=0;i<columnNames.length ;i++) {
			
			for(int j=1; j<con.size();j+=7) {
				
				String con1 = ((String) con.get(j+1)).replaceAll("\\s", ""); //type
				String conS = ((String) con.get(j)).replaceAll("\\s", ""); //columnName
				System.out.println(con1+"------------"+conS);
				
				if(conS.equals(columnNames[i])){
					System.out.println("if condition okay!!");
					String min= con.get(j+4);
		    		String max= con.get(j+5);
		    		
		    		//String type=chkType(con1);
		    		
		    		switch(con1) {
		    		
		    		case "java.lang.Integer":
		    			System.out.println("trying to reach min and max");
		    			grid.add(getRangesInt(Integer.parseInt(min),Integer.parseInt(max)));
		    			System.out.println("done with ranges 1");
		    			break;
		    		case "java.lang.Double": 
		    			 grid.add(getRangesDouble(Double.parseDouble(min),Double.parseDouble(max)));
		    			break;
		    		case "java.lang.String":
		    			 grid.add(getRangesString(min.toString(),max.toString()));
		    		
		    			break;
		    		case "java.util.Date":
		    			 Date dMin = new SimpleDateFormat("yyyy-MM-dd").parse((String) min);
		 				
		 				 Date dMax = new SimpleDateFormat("yyyy-MM-dd").parse((String) max);
		 				
		    			 grid.add(getRangesDate((Date)dMin,(Date)dMax));
		    			break;
		    		default:break;	
		    		}
//					if(!chkType ( input.get(i+1) , con1 , con.get(j+4) , con.get(j+5) )){ //check data type & range
//						output=false;
//						// ((String) input.get(i+1)).replaceAll("\\s", "")
//						throw new DBAppException();
//
//					}
						
				}
				
		}
	
		
		}
    	
    	
    	
    	
    	
    	
    	
//    	for ( int i = 0 ; i  < pageList.size() ; i++ ) {
//    		tableName=tempo;
//			pages page = (pages) pageList.get(i);
//			Vector<Tuples> tuples = page.readPage(tableName,i);
//			for(int j = 0 ; j < tuples.size() ; j++) {
//				Tuples tuple = tuples.get(j);
//				
//				
//				
//				for (int ii =0;ii<loc.length;ii++) {
//		    		for (int jj=0;jj<tableTag.size();jj++) {
//		    			if (columnNames[ii].equals(tableTag.get(jj))) {
//		    				Object min = (range[ii]).min;
//							Object max = (range[ii]).max;
//							pair p = getMinMax(tuple.tuples[loc[ii]],min,max);
//							range[ii]=p;
//		    		}}}}
//			
//			tableName = tableName +"_"+ i ; 
//			Vector overflowList = page.readOverflowVector(tableName);
//			
//			for( int k = 0 ; k  < overflowList.size() ; k++ ) {
//				overflow x = (overflow) overflowList.get(k);
//				Vector<Tuples> tuplesO = x.readPage(tableName, k);for(int j = 0 ; j < tuplesO.size() ; j++) {
//    				Tuples tuple = tuplesO.get(j);
//    				
//    				for (int iii =0;iii<loc.length;iii++) {
//    		    		for (int jjj=0;jjj<tableTag.size();jjj++) {
//    		    			if (columnNames[iii].equals(tableTag.get(jjj))) {
//    		    				Object min = (range[iii]).min;
//    							Object max = (range[iii]).max;
//    							pair p = getMinMax(tuple.tuples[loc[iii]],min,max);
//    							range[iii]=p;
//    		    		}}
//    						}}}}
    	
//    	for (int i =0;i<range.length;i++) {
//    		Object min=range[i].min;
//    		Object max=range[i].max;
//    		String type=chkType(min);
//    		switch(type) {
//    		case "java.lang.Integer":
//    			grid.add(getRangesInt((Integer)min,(Integer)max));
//    			break;
//    		case "java.lang.Double": 
//    			 grid.add(getRangesDouble((Double)min,(Double)max));
//    			break;
//    		case "java.lang.String":
//    			 grid.add(getRangesString(min.toString(),max.toString()));
//    			break;
//    		case "java.util.Date":
//    			 grid.add(getRangesDate((Date)min,(Date)max));
//    			break;
//    		default:break;	
//    		}
//    	}
    	
    	// hangeeb el tuple w nedih lel method get bucket 34an negib el esm  beta3o zai 000,010 w kedda 
    	/// then we will call method insertintobucket 
    	
    	
    	//String temp = tableName;
      	for ( int i = 0 ; i  < pageList.size() ; i++ ) {
//      	     	pages page = new pages(tableName);
      		   tableName=tempo;
    			pages page = (pages) pageList.get(i);
    			Vector<Tuples> tuples = page.readPage(tableName,i);
    			System.out.println("before loooop          tuples.size=  "+ tuples.size());
    			for(int j = 0 ; j < tuples.size() ; j++) {
    				System.out.println("after loooop");
    				Tuples tuple = tuples.get(j);
    				String bucketindex = getBucket(tuple);
    				int rowN=j;
    				String pageName=tableName+"_"+i;
    				String bucketName=tableName+"_grid"+gridN+"_"+bucketindex;
    				buckets b = new buckets();
    				b.insertinbucket(pageName, rowN , bucketName );
    				
    			}

    			tableName = tableName +"_"+ i ; 
    			Vector overflowList = page.readOverflowVector(tableName);
    			System.out.println("before loooop2");
    			for( int k = 0 ; k  < overflowList.size() ; k++ ) {
    				System.out.println("after loooop2");
    				overflow x = (overflow) overflowList.get(k);
    				Vector<Tuples> tuplesO = x.readPage(tableName, k);
    				for(int j = 0 ; j < tuplesO.size() ; j++) {
        				Tuples tuple = tuplesO.get(j);
        				String bucketindex = getBucket(tuple);
        				int rowN=j;
        				String pageName=tableName+"_"+i+"_overflow_"+k;
        				String bucketName=tableName+"_grid"+gridN+"_"+bucketindex;
        				buckets b = new buckets();
        				b.insertinbucket(pageName, rowN , bucketName );
        				}}
    			}
//    	
//    System.out.println("RANGE MIN   "+grid.get(0)[0].min + "   RANGE MAX  "+grid.get(0)[0].max  );
//    System.out.println("RANGE MIN   "+grid.get(1)[1].min + "   RANGE MAX  "+grid.get(1)[1].max  );

    }
    
    public String getBucket (Tuples t) throws ParseException {
    	String bname=""; // name of the bucket
    	Object[]a = t.tuples;
    	for (int i =0;i<loc.length;i++) //loc {1 , 8}
    	{
    		//System.out.println("loc[i]~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"+loc[i]);
    		Object x = a[loc[i]];
    		//System.out.println(x);
    		//System.out.println(loc[i]  + "==================loc[i]");
    		pair [] p = this.grid.get(i); // kedda dh el array of ranges el so8anan
    		String type=chkType(x);
    		switch(type) {
    		case "java.lang.Integer":
    			for (int j=0;j<p.length;j++) {
    				int min = (Integer)p[j].min;
    				int max = (Integer)p[j].max;
    				if ((Integer)x<=max&&(Integer)x>=min) {
    					bname+=j+"";
    				    break;
    				}
    			}
    		
    			break;
    		case "java.lang.Double": 
    			for (int j=0;j<p.length;j++) {
    				double min = (Double)p[j].min;
    				double max = (Double)p[j].max;
    				if ((Double)x<=max&&(Double)x>=min) {
    					bname+=j+"";
    				    break;
    				}
    			}
    			
    			break;
    		case "java.lang.String":
    			for (int j=0;j<p.length;j++) {
    				String min = (p[j].min).toString();
    				String max = (p[j].max).toString();
    				if(min.contains("-")){
    				}
    				else{
    					try{
    						Integer.parseInt(x.toString());
    					}
    					catch(Exception e){
    					    int xAscii=0;
    					  //  System.out.println("Are you here????");
    					    for(int s=0; s<x.toString().length(); s++)
    					    { 
    					    	int asciiValue = (x.toString()).charAt(s);
    					        xAscii = xAscii+ asciiValue;
    					   }
    					    x=xAscii+"";
    					    
    				      }
    					}
    				//System.out.println("GET BUCKET VALUE OF X:-"+ x);
    				if ((x.toString()).compareTo(min)>=0&&(x.toString()).compareTo(max)<=0) {
    					bname+=j+"";
    				    break;
    				}
    			}
    			
    			break;
    		case "java.util.Date":
    			
    			for (int j=0;j<p.length;j++) {
    				Date min = (Date) (p[j].min);
    				Date max = (Date) (p[j].max);
    				if (((Date)x).compareTo(min)>=0&&((Date)x).compareTo(max)<=0) {
    					bname+=j+"";
    				    break;
    				}
    			}
    			
    			break;
    		default:break;	
    		}
    		
    	}
    	//System.out.println(bname);
    	return bname;
    	
    }
    	
    public pair getMinMax(Object t,Object min,Object max) throws ParseException {
		if (min==null||max==null) {
			min=t;
			max=t;
			return new pair(min,max);
		}
		String type=chkType(t);
		switch(type) {
		case "java.lang.Integer":
			if ((Integer)min>(Integer)t)
				min=t;
			if ((Integer)max<(Integer)t)
				max=t;
			return new pair(min,max);
			
		case "java.lang.Double":
			if ((Double)min>(Double)t)
				min=t;
			if ((Double)max<(Double)t)
				max=t;
			return new pair(min,max);
		case "java.lang.String":
			if((t.toString()).compareTo((min.toString()))<0)
				min=t;
			if((t.toString()).compareTo((max.toString()))>0)
				max=t;
			return new pair(min,max);
		case "java.util.Date":
			if(((Date)t).compareTo((Date)min)<0)
				min=t;
			if(((Date)t).compareTo((Date)max)>0)
				max=t;
			return new pair(min,max);
			
		default:return null;
		}
	}
	
	public  String chkType(Object var) throws ParseException {
		String type = var.getClass().toString();
		String r = "";
		if (type.substring(16).equals("Integer")) {
			r = "java.lang.Integer";
		}
		if (type.substring(16).equals("Double")) {
			r = "java.lang.Double";
		}

		if (type.substring(16).equals("String")) {
			r = "java.lang.String";
		}
		if (type.substring(16).equals("Date")) {
			r = "java.util.Date";
		}

		return r;
	}
	
	public pair[] getRangesInt( int min , int max ) {
	//	System.out.println("-----------------------------------------------------GER RANGE INT-------------");
		pair[] ranges = new pair [10];
		int total_length = min - max;
		int subrange_length = total_length / 10;
		int current_start = max;
		for (int i = 0; i < 10 ; ++i) {
		 // System.out.println("Smaller range: [" + current_start + ", " + (current_start + subrange_length) + "]");
		  
			if (i==9) {
				 ranges[i] = new pair(min , current_start);
				// System.out.println("Smaller range: [" + date2 + ", " + date1 + "]");
				 }
			else {	
				ranges[i] = new pair((current_start + subrange_length)  , current_start );
				current_start += subrange_length;
		}}	
		return ranges;
	}
	
	public pair[] getRangesDouble( double min , double max ) {
		pair [] ranges = new pair [10];
		double total_length = min - max;
		double subrange_length = total_length / 10;
		double current_start = max;
		for (int i = 0; i < 10 ; ++i) {
		  ranges[i] = new pair((current_start + subrange_length)  , current_start );
		  current_start += subrange_length;
		}	
		return ranges;
	}
	
	
	public pair[] getRangesStringInt( int min , int max ) {

		pair[] ranges = new pair [10];
		int total_length = min - max;
		int subrange_length = total_length / 10;
		int current_start = max;
		for (int i = 0; i < 10 ; ++i) {
			if (i==9) {
				ranges[i] = new pair(min+"" , current_start+"");
			}
			else {	
				ranges[i] = new pair((current_start + subrange_length)+""  , current_start+"" );
				current_start += subrange_length;
			}}	
		return ranges;
	}
	
	public pair[] getRangesStringDash( String min , String max ) {
		//	System.out.println("-----------------------------------------------------GER RANGE INT-------------");
		String[] minList = min.split("-");
		String[] maxList = max.split("-");
		int len = (minList[1]).length();
		
		int minInt = Integer.parseInt(minList[0]+minList[1]+"");
		int maxInt = Integer.parseInt(maxList[0]+maxList[1]+"");
		
		
		pair[] ranges = new pair [10];
			int total_length = minInt - maxInt;
			int subrange_length = total_length / 10;
			int current_start = maxInt;
			for (int i = 0; i < 10 ; ++i) {
				 
				 String format = String.format("%4s", current_start).replace(' ', '0');
				 //System.out.println(format + " <----------------format");
				 String format2 = format.substring(2 , format.length());
				 String minFinal = (current_start+"").substring(0,2) + "-" + format2;	  
				 
				 if (i==9) {
		
					 ranges[i] = new pair( min , minFinal);
					 }
				else {	
					int y = (current_start + subrange_length);
					String newFormat = String.format("%4s", (y)).replace(' ', '0');
					newFormat =  newFormat.substring(2 , format.length());
					newFormat =  (y+"").substring(0,2) + "-" + newFormat;
				
					
					ranges[i] = new pair( newFormat  , minFinal );
					current_start += subrange_length;
			}}	
			return ranges;
		}
	
	public pair[] getRangesString (String min , String max){
		max=max.replaceAll("\\s", "");
		min=min.replaceAll("\\s", "");
		min=min.replaceAll("^\"+|\"+$", "");
		max=max.replaceAll("^\"+|\"+$", "");
	//	System.out.println(min+"    "+max);
		if(min.contains("-")){
			return getRangesStringDash(min, max);
		}
		else{
			try{
				return getRangesStringInt(Integer.parseInt(min) , Integer.parseInt(max));
			}catch(Exception e){
			    int minAscii=0;
			    int maxAscii=0;
			    for(int i=0; i<min.length(); i++)
			    { int asciiValue = min.charAt(i);
			      minAscii = minAscii+ asciiValue;
			    }
			    for(int i=0; i<max.length(); i++)
			    { int asciiValue = max.charAt(i);
			      maxAscii = maxAscii+ asciiValue;
			    }
			    return getRangesInt(minAscii, maxAscii);
		}}}

	public boolean updateGrid(String tableName, ArrayList<Object>input,int index , ArrayList<String> tableTag,String ckName,Object value,Vector<Grid>grids) throws ParseException, ClassNotFoundException, IOException {//updateGrid(tableName, input ,i,tableTag);
	
		Tuples oldt;
		Tuples newt;
		
		ArrayList<String>indexcol=new ArrayList<String>();
		
		Object[]a= new Object[input.size()/2];
		ArrayList<Object>temp=new ArrayList<Object>();
		for (int i =0;i<input.size();i++) {  
			if (i%2==0) {
				indexcol.add(input.get(i).toString());
			}
			else {
				temp.add(input.get(i));
			}
		}
		for (int i = 0;i<temp.size();i++) {
			a[i]=temp.get(i);
		}
		
	   String oldpage="";
	   Vector<Tuples> t=new Vector();
	   buckets b = new buckets() ;
	   ArrayList<String>ck=new ArrayList<String>();
	   ck.add(ckName);
	   Object[]ckval= new Object[1];
	   ckval[0]=value;
	   ArrayList<String> dbucket=getBucketDelete (ck,ckval ,tableName);
	   ArrayList<String> position=new ArrayList<String>();
	   
	   
	   for (int i =0;i<dbucket.size();i++) {
		   String bucketName = tableName+"_grid"+index+"_"+dbucket.get(i);   //bucketName=tableName+"_grid"+gridN+"_"+bucketindex;
		   Vector <pair> pair= b.readBucket(bucketName);
		   for(int j=0;j<pair.size();j++ ) {
				   oldpage=(pair.get(j).min).toString();

				   if (!(position.contains(oldpage))) {
				           position.add(oldpage);
				   }
		   }
		   
		   b.overflowBucket=b.readOverflowBucket(bucketName);
		   for(int j =0;j<b.overflowBucket.size();j++) {
		     Vector<pair> pairO=  b.readBucket(bucketName+"_overflow_"+j+"");     //bucketName+="_overflow_"+overflowBucket.size();
			 for (int k =0;k<pairO.size();k++) {
				 oldpage=(pairO.get(k).min).toString();
				   if (!(position.contains(oldpage)))
				           position.add(oldpage);
			 }
		   }
		   
		   
	   }
	
	   boolean flag=false;
	   boolean flag2=false;
	   int[]config =DBApp.getconfig();
       int n = config[1];
	   for (int i =0;i<position.size();i++) {
		   Vector<Tuples> tuplesList=readPage(position.get(i));
		   Vector<Tuples> tuplestemp= new Vector<Tuples>();
		   
		   for (int j =0;j<tuplesList.size();j++) {
			   if (!(checkdeleterow(tuplesList.get(j),tableTag,ck,ckval))) {
				   tuplestemp.add(tuplesList.get(j));
			   }
			   else {
				   String bucketName = tableName+"_grid"+index+"_"+getBucket(tuplesList.get(j));
				   Vector <pair> pair= b.readBucket(bucketName);
                   for (int k=0;k<pair.size();k++) {
                	   if ((pair.get(k)).min.toString().equals(position.get(i))){
                		     pair.remove(k);
                		     Tuples dar = tuplesList.get(j);
                		     tuplesList.remove(j);
                		     oldt=dar;
                		     dar= updateTuple(tableTag,input,dar);
                		     newt=dar;
                		     //update tuple
                		     tuplesList.add(dar);
                		     writeTupleToPage(position.get(i),tuplesList); 
                		     
                		     b.bucket=pair;
                             b.writeToBucket(bucketName, b.bucket); 
                              System.out.println("getBucket(dar) "+getBucket(dar));
                             String newBucket = tableName+"_grid"+index+"_"+getBucket(dar);
                             pair= b.readBucket(newBucket);
                             System.out.println(pair.size());
                             b.bucket=pair;
                             if (pair.size()>=n) {
                            	 b.overflowBucket=b.readOverflowBucket(bucketName);
                             	   for (int l=0;l<b.overflowBucket.size();l++) {
                               		  Vector<pair> pairO=  b.readBucket(bucketName+"_overflow_"+l+""); 
                               		  if(!(pairO.size()==n)) {
                               			  pairO.add(new pair(position.get(i),0));
                               			  b.writeToBucket(bucketName+"_overflow_"+l+"", pairO);
                               			  flag2=true;
                               			updateALLUNUSEDGrids( tableName, index , position.get(i),grids,newt,oldt);
                               			  return true;
                               			  
                               		  }
                               		  }
                             	   
                             	   
                             	   
                             	   if (!flag2) {
                             		  Vector<pair> newpair= new Vector<pair>();
                             		  newpair.add(new pair(position.get(i),0));
                             		  b.writeToBucket(bucketName+"_overflow_"+b.overflowBucket.size()+"", newpair);
                             		  
                             		  
                             		 overflowBucket o = new overflowBucket();
                                     o.bucket=newpair;
                                     b.overflowBucket.add(o);
                                     b. writeOverflowBucket(bucketName ,b.overflowBucket);
                                     updateALLUNUSEDGrids( tableName, index , position.get(i),grids,newt,oldt);
                                     return true;
                             	 }
                             }
                             else {
                            	   b.bucket.add(new pair(position.get(i),0));
                                   b.writeToBucket(newBucket, b.bucket); 
                                   updateALLUNUSEDGrids( tableName, index , position.get(i),grids,newt,oldt);
                                   return true;
                             }
                             flag=true;
                		     
                		   /////// dont forget to break !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                	   }
                   }
                   if (!flag) {
                	   b.overflowBucket=b.readOverflowBucket(bucketName);
                	   for (int k=0;k<b.overflowBucket.size();k++) {
                		   Vector<pair> pairO=  b.readBucket(bucketName+"_overflow_"+k+""); 
                		   for (int d=0;d<pairO.size();d++) {
                			   if ((pairO.get(d)).min.toString().equals(position.get(i))){
                				   pairO.remove(d);
                				   Tuples dar = tuplesList.get(j);
                				   tuplesList.remove(j);
                				   oldt=dar;
                				   dar= updateTuple(tableTag,input,dar);
                				   newt=dar;
                				   //update tuple
                				   tuplesList.add(dar);
                				   writeTupleToPage(position.get(i),tuplesList); 
                				   b.bucket=pairO;
                				   b.writeToBucket(bucketName+"_overflow_"+k+"", b.bucket); 
                				   String newBucket = tableName+"_grid"+index+"_"+getBucket(dar);
                				   pairO= b.readBucket(newBucket);
                				   b.bucket=pairO;
                                     
                                     if (pairO.size()>=n) {
                                    	 b.overflowBucket=b.readOverflowBucket(bucketName);
                                     	   for (int l=0;l<b.overflowBucket.size();l++) {
                                       		  Vector<pair> pairOO=  b.readBucket(bucketName+"_overflow_"+l+""); 
                                       		  if(!(pairOO.size()==n)) {
                                       			  pairOO.add(new pair(position.get(i),0));
                                       			  b.writeToBucket(bucketName+"_overflow_"+l+"", pairOO);
                                       			  flag2=true;
                                       			updateALLUNUSEDGrids( tableName, index , position.get(i),grids,newt,oldt);
                                       			  return true;
                                       			  
                                       		  }
                                       		  }
                                     	   
                                     	   
                                     	   
                                     	   if (!flag2) {
                                     		  Vector<pair> newpair= new Vector<pair>();
                                     		  newpair.add(new pair(position.get(i),0));
                                     		  b.writeToBucket(bucketName+"_overflow_"+b.overflowBucket.size()+"", newpair);
                                     		  
                                     		  
                                     		 overflowBucket o = new overflowBucket();
                                             o.bucket=newpair;
                                             b.overflowBucket.add(o);
                                             b. writeOverflowBucket(bucketName ,b.overflowBucket);
                                             updateALLUNUSEDGrids( tableName, index , position.get(i),grids,newt,oldt);
                                             return true;
                                     		  
                                     		  
                                     		  
                                     		  
                                     	   }
                                     }
                                     else {
                                    	 
                                    	 b.bucket.add(new pair(position.get(i),0));
                                    	 b.writeToBucket(newBucket, b.bucket);
                                    	 updateALLUNUSEDGrids( tableName, index , position.get(i),grids,newt,oldt);
                                     return true;
                                     }
                                     flag=true;
                        		     
                        	   }  } } }  } } }
	   return false;
	   }
    public void updateALLUNUSEDGrids(String tableName, int index , String pageName,Vector<Grid>grids,Tuples tuple,Tuples oldtuple) throws ParseException, ClassNotFoundException, IOException {//updateGrid(tableName, input ,i,tableTag);
 	   
    	
    	int[]config =DBApp.getconfig();
       int n = config[1];
       boolean flag=false;
    	boolean flag2=false;
    	for (int z =0;z<grids.size();z++) {
			if (z!=index) {
				   buckets b = new buckets();
				   String bucketName = tableName+"_grid"+z+"_"+getBucket(oldtuple);
				   Vector <pair> pair= b.readBucket(bucketName);
				   
                   for (int k=0;k<pair.size();k++) {
                	   if ((pair.get(k)).min.toString().equals(pageName)){
                		     pair.remove(k);
                		  
                		   b.bucket=pair;
                           b.writeToBucket(bucketName, b.bucket); 
							String newBucket = tableName+"_grid"+z+"_"+getBucket(tuple);
                             pair= b.readBucket(newBucket);
                             System.out.println(pair.size());
                             b.bucket=pair;
                             if (pair.size()>=n) {
                            	 b.overflowBucket=b.readOverflowBucket(bucketName);
                             	   for (int l=0;l<b.overflowBucket.size();l++) {
                               		  Vector<pair> pairO=  b.readBucket(bucketName+"_overflow_"+l+""); 
                               		  if(!(pairO.size()==n)) {
                               			  pairO.add(new pair(pageName,0));
                               			  b.writeToBucket(bucketName+"_overflow_"+l+"", pairO);
                               			  flag2=true;
                               			  return;
                               			  
                               		  }
                               		  }
                             	    if (!flag2) {
                             		  Vector<pair> newpair= new Vector<pair>();
                             		  newpair.add(new pair(pageName,0));
                             		  b.writeToBucket(bucketName+"_overflow_"+b.overflowBucket.size()+"", newpair);
                             		  
                             		  
                             		 overflowBucket o = new overflowBucket();
                                     o.bucket=newpair;
                                     b.overflowBucket.add(o);
                                     b. writeOverflowBucket(bucketName ,b.overflowBucket);
                                     return;
                             		  
                             	   }
                             }
                             else {
                            	   b.bucket.add(new pair(pageName,0));
                                   b.writeToBucket(newBucket, b.bucket); 
                                   return;
                             }
                             flag=true;
                		     
                	   }
                   }
                   if (!flag) {
                	   b.overflowBucket=b.readOverflowBucket(bucketName);
                	   for (int k=0;k<b.overflowBucket.size();k++) {
                		   Vector<pair> pairO=  b.readBucket(bucketName+"_overflow_"+k+""); 
                		   for (int d=0;d<pairO.size();d++) {
                			   if ((pairO.get(d)).min.toString().equals(pageName)){
                				   pairO.remove(d);
                				   b.bucket=pairO;
                				   b.writeToBucket(bucketName+"_overflow_"+k+"", b.bucket); 
                				   String newBucket = tableName+"_grid"+z+"_"+getBucket(tuple);
                				   pairO= b.readBucket(newBucket);
                				   b.bucket=pairO;
                                     
                                     if (pairO.size()>=n) {
                                    	 b.overflowBucket=b.readOverflowBucket(bucketName);
                                     	   for (int l=0;l<b.overflowBucket.size();l++) {
                                       		  Vector<pair> pairOO=  b.readBucket(bucketName+"_overflow_"+l+""); 
                                       		  if(!(pairOO.size()==n)) {
                                       			  pairOO.add(new pair(pageName,0));
                                       			  b.writeToBucket(bucketName+"_overflow_"+l+"", pairOO);
                                       			  flag2=true;
                                       			  return;
                                       			  
                                       		  }
                                       		  }
                                     	   if (!flag2) {
                                     		  Vector<pair> newpair= new Vector<pair>();
                                     		  newpair.add(new pair(pageName,0));
                                     		  b.writeToBucket(bucketName+"_overflow_"+b.overflowBucket.size()+"", newpair);
                                     		  
                                     		  
                                     		 overflowBucket o = new overflowBucket();
                                             o.bucket=newpair;
                                             b.overflowBucket.add(o);
                                             b. writeOverflowBucket(bucketName ,b.overflowBucket);
                                             return;
                                     		  
                                     	   }
                                     }
                                     else {
                                    	 
                                    	 b.bucket.add(new pair(pageName,0));
                                    	 b.writeToBucket(newBucket, b.bucket);
                                     return;
                                     }
                                     flag=true;
                        		     
                        	   }}}}}}
    } 
		
    public void updateALLGrids(String tableName,  String pageName ,Vector<Grid>grids,Tuples tuple,Tuples oldtuple) throws ParseException, ClassNotFoundException, IOException {//updateGrid(tableName, input ,i,tableTag);
 	   
    	
    	int[]config =DBApp.getconfig();
       int n = config[1];
       boolean flag=false;
    	boolean flag2=false;
    	for (int z =0;z<grids.size();z++) {
			
				   buckets b = new buckets();
				   String bucketName = tableName+"_grid"+z+"_"+getBucket(oldtuple);
				   Vector <pair> pair= b.readBucket(bucketName);
				   
                   for (int k=0;k<pair.size();k++) {
                	   if ((pair.get(k)).min.toString().equals(pageName)){
                		     pair.remove(k);
                		  
                		   b.bucket=pair;
                           b.writeToBucket(bucketName, b.bucket); 
							String newBucket = tableName+"_grid"+z+"_"+getBucket(tuple);
                             pair= b.readBucket(newBucket);
                             System.out.println(pair.size());
                             b.bucket=pair;
                             if (pair.size()>=n) {
                            	 b.overflowBucket=b.readOverflowBucket(bucketName);
                             	   for (int l=0;l<b.overflowBucket.size();l++) {
                               		  Vector<pair> pairO=  b.readBucket(bucketName+"_overflow_"+l+""); 
                               		  if(!(pairO.size()==n)) {
                               			  pairO.add(new pair(pageName,0));
                               			  b.writeToBucket(bucketName+"_overflow_"+l+"", pairO);
                               			  flag2=true;
                               			  return;
                               			  
                               		  }
                               		  }
                             	    if (!flag2) {
                             		  Vector<pair> newpair= new Vector<pair>();
                             		  newpair.add(new pair(pageName,0));
                             		  b.writeToBucket(bucketName+"_overflow_"+b.overflowBucket.size()+"", newpair);
                             		  
                             		  
                             		 overflowBucket o = new overflowBucket();
                                     o.bucket=newpair;
                                     b.overflowBucket.add(o);
                                     b. writeOverflowBucket(bucketName ,b.overflowBucket);
                                     return;
                             		  
                             	   }
                             }
                             else {
                            	   b.bucket.add(new pair(pageName,0));
                                   b.writeToBucket(newBucket, b.bucket); 
                                   return;
                             }
                             flag=true;
                		     
                	   }
                   }
                   if (!flag) {
                	   b.overflowBucket=b.readOverflowBucket(bucketName);
                	   for (int k=0;k<b.overflowBucket.size();k++) {
                		   Vector<pair> pairO=  b.readBucket(bucketName+"_overflow_"+k+""); 
                		   for (int d=0;d<pairO.size();d++) {
                			   if ((pairO.get(d)).min.toString().equals(pageName)){
                				   pairO.remove(d);
                				   b.bucket=pairO;
                				   b.writeToBucket(bucketName+"_overflow_"+k+"", b.bucket); 
                				   String newBucket = tableName+"_grid"+z+"_"+getBucket(tuple);
                				   pairO= b.readBucket(newBucket);
                				   b.bucket=pairO;
                                     
                                     if (pairO.size()>=n) {
                                    	 b.overflowBucket=b.readOverflowBucket(bucketName);
                                     	   for (int l=0;l<b.overflowBucket.size();l++) {
                                       		  Vector<pair> pairOO=  b.readBucket(bucketName+"_overflow_"+l+""); 
                                       		  if(!(pairOO.size()==n)) {
                                       			  pairOO.add(new pair(pageName,0));
                                       			  b.writeToBucket(bucketName+"_overflow_"+l+"", pairOO);
                                       			  flag2=true;
                                       			  return;
                                       			  
                                       		  }
                                       		  }
                                     	   if (!flag2) {
                                     		  Vector<pair> newpair= new Vector<pair>();
                                     		  newpair.add(new pair(pageName,0));
                                     		  b.writeToBucket(bucketName+"_overflow_"+b.overflowBucket.size()+"", newpair);
                                     		  
                                     		  
                                     		 overflowBucket o = new overflowBucket();
                                             o.bucket=newpair;
                                             b.overflowBucket.add(o);
                                             b. writeOverflowBucket(bucketName ,b.overflowBucket);
                                             return;
                                     		  
                                     	   }
                                     }
                                     else {
                                    	 
                                    	 b.bucket.add(new pair(pageName,0));
                                    	 b.writeToBucket(newBucket, b.bucket);
                                     return;
                                     }
                                     flag=true;
                        		     
                        	   }}}}}
    }	

	public Tuples updateTuple( ArrayList<String> tableTag , ArrayList<Object> input , Tuples t ) {
	//	System.out.println("size " + input.size());
		Tuples x = t;
		for (int i =0;i<tableTag.size();i++) {
			int j=0;
		//	System.out.println("table tag      "+ tableTag.get(i));
			while (j<input.size()) {
				if (tableTag.get(i).equals(input.get(j).toString())) {		
			//		System.out.println("j " + input.get(j));
				//	System.out.println("j+1 " + input.get(j+1));
					//System.out.println(x.tuples[i]);
					x.tuples[i]=input.get(j+1);
				}
				j=j+2;
			}
		}
	//	System.out.println(x);
		return x;
	}

    public boolean  insertInGrid(Tuples tuple,String tableName,ArrayList<String>tableTag,int gridNumber, String ClustringKeyColoumn, Object[] ckval,ArrayList<String>dbucket,Hashtable<String, Object> colNameValue) throws ClassNotFoundException, IOException, ParseException {
     
       ArrayList<String>indexcol=new ArrayList<String>();
       String oldpage="";
	   ArrayList<String>ck=new ArrayList<String>();
	   ck.add(ClustringKeyColoumn);
	   ArrayList<String> position=new ArrayList<String>();
	   
	   
	   for (int i =0;i<dbucket.size();i++) {
		   String bucketName = tableName+"_grid"+gridNumber+"_"+dbucket.get(i);   //bucketName=tableName+"_grid"+gridN+"_"+bucketindex;
		   buckets b = new buckets();
		   Vector <pair> pair= b.readBucket(bucketName);
		   for(int j=0;j<pair.size();j++ ) {
				   oldpage=(pair.get(j).min).toString();

				   if (!(position.contains(oldpage))) {
				           position.add(oldpage);
				   }
		   }
		   b.overflowBucket=b.readOverflowBucket(bucketName);
		   for(int j =0;j<b.overflowBucket.size();j++) {
		     Vector<pair> pairO=  b.readBucket(bucketName+"_overflow_"+j+"");     //bucketName+="_overflow_"+overflowBucket.size();
			 for (int k =0;k<pairO.size();k++) {
				 oldpage=(pairO.get(k).min).toString();
				   if (!(position.contains(oldpage)))
				           position.add(oldpage);
			 }
		   }
		}
	
	   boolean flag=false;
	   boolean flag2=false;
	   int[]config =DBApp.getconfig();
       int n = config[1];
       Vector<pages>pages= readTablePages(tableName);

	   Object x=ckval[0];
	   
	   for (int i =0;i<position.size();i++) {
		  String [] y =position.get(i).split("_");
		  pages p = pages.get(Integer.parseInt(y[1]));
		  
		  String type=chkType(p.max);
		  switch(type) {
		  case "java.lang.Integer":

			  int min = (Integer)p.min;
			  int max = (Integer)p.max;
			  if ((Integer)x<=max&&(Integer)x>=min) {
				  p.insertToPageGrid(false,Integer.parseInt(y[1]), tableName, colNameValue, ClustringKeyColoumn, tableTag);
				  return true;

			  }

			  break;
		  case "java.lang.Double": 

			  double mind = (Double)p.min;
			  double maxd = (Double)p.max;
			  if ((Double)x<=maxd&&(Double)x>=mind) {

				  p.insertToPageGrid(false,Integer.parseInt(y[1]), tableName, colNameValue, ClustringKeyColoumn, tableTag);
				 return true;

			  }

			  break;
		  case "java.lang.String":

			  String mins = (p.min).toString();
			  String maxs = (p.max).toString();
			  
			  if ((x.toString()).compareTo(mins)>=0&&(x.toString()).compareTo(maxs)<=0) {

				  p.insertToPageGrid(false,Integer.parseInt(y[1]), tableName, colNameValue, ClustringKeyColoumn, tableTag);
				return true;
			  }

			  break;
		  case "java.util.Date":


			  Date minD = (Date) (p.min);
			  Date maxD = (Date) (p.max);
			  if (((Date)x).compareTo(minD)>=0&&((Date)x).compareTo(maxD)<=0) {

				  p.insertToPageGrid(false,Integer.parseInt(y[1]), tableName, colNameValue, ClustringKeyColoumn, tableTag);
				  return true;
			  }


			  break;
		  default:break;	
		  }
		  
		  }
	   return false;
		}

    public void insertInBucket(String pageName, Tuples t, String tableName,Vector<Grid>grids) throws ParseException, FileNotFoundException, IOException, ClassNotFoundException {
    	boolean flag=false;
    	String gridsName = tableName+"_grids";
 	    int[]config =DBApp.getconfig();
       int n = config[1];
    	for (int i=0;i<grids.size();i++) {
    		
    			buckets b = new buckets();
    			String bucketName = tableName+"_grid"+i+"_"+getBucket(t);
    			Vector <pair> pair= b.readBucket(bucketName);
    		
    			
    			if (pair.size()<n) {
    				pair.add(new pair(pageName,0));
    				b.bucket=pair;
    				b.writeToBucket(bucketName, b.bucket); 
    				
    			}
    			else {
    				b.overflowBucket=b.readOverflowBucket(bucketName);
    				for (int k=0;k<b.overflowBucket.size();k++) {

    					Vector<pair> pairO=  b.readBucket(bucketName+"_overflow_"+k+""); 
    					
    					if (pairO.size()<n) {
    						b.bucket=pair;
    						b.writeToBucket(bucketName+"_overflow_"+k+"", b.bucket); 
    						flag=true;
    					 	b.writeOverflowBucket(tableName+"_grid"+i+"_"+getBucket(t),b.overflowBucket);
    						}
    				}
    				
    			      if (!flag) {
    			    	    overflowBucket o = new overflowBucket();
    	                	bucketName+="_overflow_"+b.overflowBucket.size();
    	                	o.bucket=new Vector<pair>();
    	                	o.bucket.add(new pair(pageName,0));
//    	                	o.insertinbucket(pageName, rowN,bucketName);
    	                	b.overflowBucket.add(o);
    	                	b.writeToBucket(bucketName,o.bucket);
    	                	b.writeOverflowBucket(tableName+"_grid"+i+"_"+getBucket(t),b.overflowBucket);
    			    	  
    			      }}
    			
    			}}
    	
   
    public static String convertStringToBinary(String input) {

        StringBuilder result = new StringBuilder();
        char[] chars = input.toCharArray();
        for (char aChar : chars) {
            result.append(
                    String.format("%8s", Integer.toBinaryString(aChar))   // char -> int, auto-cast
                            .replaceAll(" ", "0")                         // zero pads
            );
        }
        return result.toString();

    }
	
	public pair[] getRangesDate(Date min,Date max) {
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String mins = dateFormat.format(min);
        String maxs = dateFormat.format(max);
	    LocalDate minDate = LocalDate.parse( mins);
	    LocalDate maxDate = LocalDate.parse( maxs);
	    int diff = (int)ChronoUnit.DAYS.between( maxDate , minDate );
		pair[] ranges = new pair [10];
		ZoneId defaultZoneId = ZoneId.systemDefault();
		int sdiff = diff / 10;
		LocalDate current_start = maxDate;
		for (int i = 0; i < 10 ; ++i) {
			
	    Date date1 = Date.from(minDate .atStartOfDay(defaultZoneId).toInstant());
	    Date date2 = Date.from(current_start.atStartOfDay(defaultZoneId).toInstant());
	    Date date3 = Date.from((current_start.plusDays(sdiff)).atStartOfDay(defaultZoneId).toInstant());
		if (i==9) {
			 ranges[i] = new pair(date1, date2);
			// System.out.println("Smaller range: [" + date2 + ", " + date1 + "]");
			 }
		else {
		//	System.out.println("Smaller range: [" + date2 + ", " + date3+ "]");
		  ranges[i] = new pair(date3 , date2);
		  current_start= (current_start.plusDays(sdiff));
		  }
		}	
		return ranges;

	}
	
	public void deletefromGrid(String tableName, ArrayList columnNameValue,int index, ArrayList<String> tableTag) throws ParseException, ClassNotFoundException, IOException {
		
		ArrayList<String>indexcol=new ArrayList<String>();
		Object[]a= new Object[columnNameValue.size()/2];
		ArrayList<Object>temp=new ArrayList<Object>();
		for (int i =0;i<columnNameValue.size();i++) {  
			if (i%2==0) {
				indexcol.add(columnNameValue.get(i).toString());
			}
			else {
				temp.add(columnNameValue.get(i));
			}
		}
		for (int i = 0;i<temp.size();i++) {
			a[i]=temp.get(i);
		}
	   String oldpage="";
	   Vector<Tuples> t=new Vector();
	   buckets b = new buckets() ;
	   ArrayList<String> dbucket=getBucketDelete (indexcol,a, tableName);
	   ArrayList<String> position=new ArrayList<String>();
	   
	   for (int i =0;i<dbucket.size();i++) {
		   String bucketName = tableName+"_grid"+index+"_"+dbucket.get(i);   //bucketName=tableName+"_grid"+gridN+"_"+bucketindex;
		   Vector <pair> pair= b.readBucket(bucketName);
		   for(int j=0;j<pair.size();j++ ) {
				   oldpage=(pair.get(j).min).toString();
				   if (!(position.contains(oldpage)))
				           position.add(oldpage);
			  
		   }
		   
		   b.overflowBucket=b.readOverflowBucket(bucketName);
		   for(int j =0;j<b.overflowBucket.size();j++) {
		     Vector<pair> pairO=  b.readBucket(bucketName+"_overflow_"+j+"");     //bucketName+="_overflow_"+overflowBucket.size();
			 for (int k =0;k<pairO.size();k++) {
				 oldpage=(pairO.get(k).min).toString();
				   if (!(position.contains(oldpage)))
				           position.add(oldpage);
			 }
		   }
		   
		   
	   }
	
	   boolean flag=false;
	   for (int i =0;i<position.size();i++) {
		   Vector<Tuples> tuplesList=readPage(position.get(i));
		   Vector<Tuples> tuplestemp= new Vector<Tuples>();
		   
		   for (int j =0;j<tuplesList.size();j++) {
			   if (!(checkdeleterow(tuplesList.get(j),tableTag,indexcol,a))) {
				   tuplestemp.add(tuplesList.get(j));
			   }
			   else {
				   String bucketName = tableName+"_grid"+index+"_"+getBucket(tuplesList.get(j));
				   Vector <pair> pair= b.readBucket(bucketName);
                   for (int k=0;k<pair.size();k++) {
                	   if ((pair.get(k)).min.toString().equals(position.get(i))){
                		     pair.remove(k);
                		     flag=true;
                	   }
                   }
                   if (flag) {
                	   deletefromALLGrids(tableName,index,tuplesList.get(j),position.get(i));  //tuple= tuplesList.get(j)-----> the tuple that we want to delete; w hena bardo
                	   b.bucket=pair;
                       b.writeToBucket(bucketName, b.bucket); 
                       flag=false;
                   }
                   else {
                	   b.overflowBucket=b.readOverflowBucket(bucketName);
                	   for (int k=0;k<b.overflowBucket.size();k++) {
                		
                		   Vector<pair> pairO=  b.readBucket(bucketName+"_overflow_"+k+""); 
                           for (int d=0;d<pairO.size();d++) {
                        	   if ((pairO.get(d)).min.toString().equals(position.get(i))){
                        		     pairO.remove(d);
                        		     flag=true;
                        	   }
                           }
                           if (flag) {
                        	   deletefromALLGrids(tableName,index,tuplesList.get(j),position.get(i)); ///// hena ncall el method 
                        	   b.bucket=pair;
                               b.writeToBucket(bucketName+"_overflow_"+k+"", b.bucket); 
                               flag=false;
                           }
                           }}}}
		  
		  writeTupleToPage(position.get(i),tuplestemp); 
		  
	   } 
	}

    public void deletefromALLGrids(String tableName, int index, Tuples tuple,String pageName) throws ParseException, ClassNotFoundException, IOException {
    	boolean flag=false;
    	String gridsName = tableName+"_grids";
    	Vector<Grid> grids = readGrids(gridsName);
    	for (int i=0;i<grids.size();i++) {
    		if (i!=index) {
    			buckets b = new buckets();
    			String bucketName = tableName+"_grid"+i+"_"+getBucket(tuple);
    			Vector <pair> pair= b.readBucket(bucketName);
    			for (int k=0;k<pair.size();k++) {
    				if ((pair.get(k)).min.toString().equals(pageName)){
    					pair.remove(k);
    					flag=true;
    				}
    			}
    			if (flag) {
    				b.bucket=pair;
    				b.writeToBucket(bucketName, b.bucket); 
    				flag=false;
    			}
    			else {
    				b.overflowBucket=b.readOverflowBucket(bucketName);
    				for (int k=0;k<b.overflowBucket.size();k++) {

    					Vector<pair> pairO=  b.readBucket(bucketName+"_overflow_"+k+""); 
    					for (int d=0;d<pairO.size();d++) {
    						if ((pairO.get(d)).min.toString().equals(pageName)){
    							pairO.remove(d);
    							flag=true;
    						}
    					}
    					if (flag) {
    						b.bucket=pair;
    						b.writeToBucket(bucketName+"_overflow_"+k+"", b.bucket); 
    						flag=false;
    					} }}}}} 

	public boolean checkdeleterow(Tuples t , ArrayList<String> tableTag,ArrayList<String> indexcol,Object[]a  ) throws ParseException { 
		for (int i=0;i<indexcol.size();i++) {
			int n= tableTag.indexOf(indexcol.get(i));
			String type= chkType(t.tuples[n]);
			switch(type) {
    		case "java.lang.Integer":
    			if (!((Integer)t.tuples[n]==(Integer)a[i]))
    				return false;
    			break;
    		case "java.lang.Double": 
    			if (!((Double)t.tuples[n]==(Double)a[i]))
    				return false;
    			break;
    		case "java.lang.String":
    			if (!(t.tuples[n].toString().equals(a[i].toString())))
    				return false;
    			break;
    		case "java.util.Date":
    			Date d = new SimpleDateFormat("yyyy-MM-dd").parse((String) a[i]);
    			if (!(((Date)t.tuples[n]).equals((Date)d)))
    				return false;
    			break;
    		default:break;	
    		}
			
		}
		return true;
	}
	
	public  void writeTupleToPage(String pageName , Vector  t) throws FileNotFoundException, IOException {
		String fileName = "src/main/resources/data/" + pageName+ ".pages";
	    try {
	    	ObjectOutputStream os =new ObjectOutputStream(new FileOutputStream(fileName) );
	    	os.writeObject(t);
	    	os.close();	
		} catch (Exception e) {
			// TODO: handle exception
		}    
  }

	public  Vector<Tuples> readPage (String pageName) throws IOException, ClassNotFoundException {
		String fileName = "src/main/resources/data/"+pageName+".pages";
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
	
	public ArrayList<String> getBucketDelete (ArrayList <String> indexcol, Object[]a ,String tableName) throws ParseException, IOException {
		String bname=""; // name of the bucket
		ArrayList<String> buck=new ArrayList<String>();
		buck.add(bname);
		boolean flag= false;
		Object x=null;
    	for (int i =0;i<columnNames.length;i++) 
    	{   
    		String n=columnNames[i];
    	    for (int k=0;k<indexcol.size();k++) {
    	    	if (n.equals(indexcol.get(k))) {
    	    		 x = a[k];
    	    		flag=true;
    	    	//	System.out.println(x + "xxxx");
    	    	}
    	    }
    	    if (flag) {
    	    flag=false;
    		pair [] p = this.grid.get(i);// kedda dh el array of ranges el so8anan
    		//System.out.println("in pairs -------"+p[0].min);
    		String type=ckType(tableName);
    	//	System.out.println(type);
    		switch(type) {
    		case "java.lang.Integer":
    			for (int j=0;j<p.length;j++) {
    				int min = (Integer)p[j].min;
    				int max = (Integer)p[j].max;
    				if ((Integer)x<=max&&(Integer)x>=min) {
    					buck=concat(buck,j);
    					break;
    				}
    			}
    		
    			break;
    		case "java.lang.Double": 
    			for (int j=0;j<p.length;j++) {
    				double min = (Double)p[j].min;
    				double max = (Double)p[j].max;
    				if ((Double)x<=max&&(Double)x>=min) {
    					buck=concat(buck,j);
    					break;
    				}
    			}
    			
    			break;
    		case "java.lang.String":
    			for (int j=0;j<p.length;j++) {
    				String min = (p[j].min).toString();
    				String max = (p[j].max).toString();
    				
    				if(min.contains("-")){
    				}
    				else{
    					try{
    						Integer.parseInt(x.toString());
    					}
    					catch(Exception e){
    					    int xAscii=0;
    					    for(int s=0; s<x.toString().length(); s++)
    					    { 
    					    	int asciiValue = x.toString().charAt(s);
    					        xAscii = xAscii+ asciiValue;
    					   }
    					    x=xAscii+"";
    				      }
    					}
    			
    				if ((x.toString()).compareTo(min)>=0&&(x.toString()).compareTo(max)<=0) {
    					buck=concat(buck,j);
    				    break;
    				}
    			}
    			
    			break;
    		case "java.util.Date":
    			for (int j=0;j<p.length;j++) {
    				Date min = (Date) (p[j].min);
    				Date max = (Date) (p[j].max);
    				Date xx = new SimpleDateFormat("yyyy-MM-dd").parse((String) x);
    				if (((Date)xx).compareTo(min)>=0&&((Date)xx).compareTo(max)<=0) {
    					buck=concat(buck,j);
    				    break;
    				}
    			}
    			
    			break;
    		default:break;	
    		}
    		
    	}
    	    else {
    	    	ArrayList<String>temp=new ArrayList<String>();
    	    	for(int g=0;g<buck.size();g++ ) {
    	    		String t=buck.get(g);
    	    		for (int l=0;l<10;l++) {
    	    			String in = t+l+"";
    	    			temp.add(in);
    	    		}
    	    	}
    	    	buck=temp;
    	    }
    	
    	
	}
    	return buck;
	}

	
	public static ArrayList<String> concat(ArrayList<String>a, int j ){
		ArrayList<String> outp = new ArrayList<String>();
		for (int i =0;i<a.size();i++) {
			String temp=a.get(i)+j+"";
			outp.add(temp);
		}
		return outp;
	}
	
	
	public String ckType(String tableName) throws IOException {
		String ck = "";
		BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
		String current = "";
		while ((current = br.readLine()) != null) {
			String[] line = current.split(",");
			for (int i = 0; i < line.length; i += 7) {
				String name = ((String) line[i]).replaceAll("\\s", "");
				String input = ((String) line[i + 3]).replaceAll("\\s", "");

				if (name.equals(tableName)) {
					if ((input.toString()).equalsIgnoreCase("True")) {
						ck = ((String) line[i + 2]).replaceAll("\\s", "");
						return ck;
					}
				}
			}
		}

		return ck;

	}
	
	
	public Vector readTablePages( String tableName) throws FileNotFoundException, IOException, ClassNotFoundException {

		String fileName = "src/main/resources/data/"+tableName+".txt";
		Vector tt = new pages(tableName);
		Table t;
        File file = new File(fileName);
        if (file.exists()) {
        	 FileInputStream fileStream = new FileInputStream(fileName);
     	     ObjectInputStream   is = new ObjectInputStream(fileStream);
     	     tt = (Vector) is.readObject();
     	     return tt ;
     	     }  
        Vector p = new pages(tableName);
        return p;   
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
	public static void main(String[] args) throws ParseException {
     
		//System.out.println("gego"^"gego");
//		String bname="0"; // name of the bucket
//		ArrayList<String> bucks=new ArrayList<String>();
//		bucks.add(bname);
//		bucks.add("1");
		
		//System.out.println("Before :- "+ );
//		String [] columnNames= {"gpa","age"};
//		pair[] pair1= {new pair(new Double(0.0),new Double(2.0)),new pair(new Double(2.0),new Double(3.5)),new pair(new Double(3.5),new Double(5.0))};
//		pair[] pair2= {new pair(0,10),new pair(10,20),new pair(20,30)};
//		Vector<pair[]> g=new Vector<pair[]>();
//		g.add(pair1);
//		g.add(pair2);
//		Grid grid = new Grid(columnNames);
//		
//		pair[]p=grid.getRangesString(" AAAAAA", "zzzzzz");
//		for (int i=0;i<p.length;i++) {
//			System.out.println(p[i].min+"  "+p[i].max);
//		}
//		int minAscii=0;
//	    int maxAscii=0;
//	    for(int i=0; i<"a".length(); i++)
//	    { int asciiValue = "a".charAt(i);
//	      minAscii = minAscii+ asciiValue;
//	    }
//	    System.out.println(minAscii);
//		System.out.println(grid.loc.length);
																					//String tableName, ArrayList columnNameValue,int index, ArrayList<String> tableTag
//		grid.grid=g;
//		ArrayList<String>indexcol = new ArrayList();
//		indexcol.add("age");
//		indexcol.add("gpa");
		
		//grid.deletefromGrid(tableName, indexcol, index, tableTag);
		                                             //(String tableName, String[] columnNames,Table t,int gridN)
	
		//Object[]a= {21,2.1};
		
	//	System.out.println((Double)(grid.grid.get(0)[0].max));
	//bucks=grid.getBucketDelete(indexcol, a);
		
		
		
//		for (int i =0;i<bucks.size();i++) {
//			System.out.println("ana honaaaaaaaaaaaaaaaaaaaaaaaaaa"+bucks.get(i));
//		}
//
//		

		//System.out.println("After :- "+bucks.get(0)+"-------"+bucks.get(1));
//		Grid grid = new Grid();
//		pair[] p = grid.getRangesString( "AAAA" , "ZZZZ" );
//		for(int j = 0 ; j < 10 ; j++) {
//			//if(  4 >= (double )p[j].min && 4 <= (double )p[j].max) {
//				System.out.println(p[j].min + " <-----> " + p[j].max);
//		}
//		
//		String min="2018-12-24";
//		String max="2019-12-24";
//		
//		 Date dMin = new SimpleDateFormat("yyyy-MM-dd").parse((String) min);
//			
//	     Date dMax = new SimpleDateFormat("yyyy-MM-dd").parse((String) max);
//	     grid.getRangesDate(dMin, dMax);
		
		
		
	}

}
