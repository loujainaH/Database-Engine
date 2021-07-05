import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Locale;
import java.util.Vector;
import java.util.Scanner;

import java.text.SimpleDateFormat;
import java.util.Date;


public class Table implements Serializable {
	int pagesCount;
	String name;
	String ClustringKeyColoumn;
	Hashtable<String, String> htblColNameType;
	transient Vector page;
	Vector<Grid> grids;

	public Table(String tableName) throws IOException {
		this.name = tableName;
		this.ClustringKeyColoumn = ck(tableName);
		this.page = new pages(tableName);
		this.pagesCount = 0;
		this.grids=new Vector<Grid>();
	}
	
	public  void writeTablePages( Vector  t , String tableName) throws FileNotFoundException, IOException {

		String fileName = "src/main/resources/data/"+tableName+".txt";
	    try {
	    	ObjectOutputStream os =new ObjectOutputStream(new FileOutputStream(fileName) );
	    	os.writeObject(t);
	    	os.close();	
		} catch (Exception e) {
			// TODO: handle exception
		}    
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
	
	public  void insertInTable(String tableName, Hashtable<String, Object> colNameValue)throws IOException, ParseException, ClassNotFoundException {
		String gridsName = tableName+"_grids";
		this.grids = readGrids(gridsName);
		
		this.page = readTablePages(tableName); 
//		this.pagesCount = page.size();
		System.out.println("PAGE SIZE "+ page.size());
		
		int[]config =DBApp.getconfig();
		int n = config[0];
		ClustringKeyColoumn=ck(tableName);
		Object o = getCkValue(colNameValue,ClustringKeyColoumn);
		ArrayList<String> tableTag= getOrder(tableName);
		String type= chkType(o);
		//System.out.println("el clustering key "+ClustringKeyColoumn);
		pagesCount=this.page.size();
		
		boolean useGrid=false;
		int gridNumber=0;
		for (int i =0;i<this.grids.size();i++) {
			String [] columnNames= this.grids.get(i).columnNames;
			for (int j=0;j<columnNames.length;j++) {
				if (columnNames[j].equals(ClustringKeyColoumn)) {
					
					useGrid=true;
					gridNumber=i;
					
					Tuples t= new Tuples(colNameValue, ClustringKeyColoumn, tableTag); 
					Grid g = this.grids.get(gridNumber);
					
					
					ArrayList<String>indexcol=new ArrayList<String>();
					Enumeration<String> values1 = colNameValue.keys();
					Enumeration<Object> values2 = colNameValue.elements();
					ArrayList<Object>temp=new ArrayList<Object>();
					this.page = readTablePages(tableName);
					String ckName = ck(tableName);
					while (values1.hasMoreElements()) {
						String data = values1.nextElement().toString();
						Object data2 = values2.nextElement();
						indexcol.add(data);
						temp.add(data2);
						}
					
					Object[]a= new Object[temp.size()];
					
					for (int k = 0;k<temp.size();k++) {
						a[k]=temp.get(k);
					}
					
				   String oldpage="";
				   
				   ArrayList<String>ck=new ArrayList<String>();
				   ck.add(ClustringKeyColoumn);
				   Object[]ckval= new Object[1];
				   ckval[0]=ckval;
				   ArrayList<String> dbucket=g.getBucketDelete (ck,ckval ,tableName);
				
				
					if (dbucket.size()==0) {
						useGrid=false;
						
					}
					else {
					
						
						useGrid=g.insertInGrid(t,tableName,tableTag,gridNumber,ClustringKeyColoumn,ckval, dbucket,colNameValue);
					}
					
					
					break;
				}//////
			}
		}
		
		if (!useGrid) {
		           
			switch(type) {
		case "java.lang.Integer":
			{
				//1st page to be created
				if(pagesCount == 0)
					
				{ 
				  page=new pages(tableName);
			      pages p = new pages(tableName);
			      page.add(p);
			      pagesCount= pagesCount+1;
			      p.insertToPageGrid(false, 0 , tableName, colNameValue, ClustringKeyColoumn, tableTag);
			    }
				//Some pages already exist
				else {

					String ckName = ck(tableName); 
					Object ckVal = getCkValue( colNameValue , ckName );			
					{
						   if (page.size()==1) {
							 //  System.out.println("***************************8pageSize==1***************************");
							   Tuples t= new Tuples(colNameValue, ckName, tableTag);
							   int z = (int) t.clusterKey;
							   pages p1=(pages) page.get(0);
							   p1.tuples=p1.readPage(tableName, 0);
							   if ((p1).tuples.size()==n && z > ((int)p1.max) ) {
								   	  pages newPage = new pages(tableName);
								      page.add(newPage);
								      pagesCount =pagesCount+1;
								      newPage.insertToPageGrid( false,1 ,tableName , colNameValue , ckName , tableTag);
							   }
							   else {
								   pages iPage = (pages) page.get(0);
								   
								   boolean chkRepeated = checkRepeatedKeyInt( iPage , (int) ckVal , tableName , 0 );
//								   if(!chkRepeated ) {
									   iPage.insertToPageGrid( false,0 ,tableName , colNameValue , ckName , tableTag);
//								   }
							   }
							   }
						   else {
			   
							   if( pagesCount == 2) {
								   
								   Tuples t= new Tuples(colNameValue, ckName, tableTag);
								   int z = (int) t.clusterKey;
								   pages p1=(pages) page.get(0);
								   pages p2=(pages) page.get(1);

								   	if( z >    (int) p2.max) {
									   p2.tuples = readPageTable(tableName, 1);
									   if(p2.tuples.size() >= n ) {
										    pages ppp = new pages(tableName);
											page.add(ppp);
										    pagesCount= pagesCount+1;
											ppp.insertToPageGrid(false, 2 , tableName, colNameValue, ckName, tableTag);   
									   }
									   else {
										   boolean chkRepeated = checkRepeatedKeyInt( p2 , (int) ckVal , tableName , 1 );
										   if(!chkRepeated ) {
											   p2.insertToPageGrid(false, 1, tableName, colNameValue, ckName, tableTag);
										   }}
								   	}else {
										   if( z <(int) p2.min) {
											   boolean chkRepeated = checkRepeatedKeyInt( p1 , (int) ckVal , tableName , 0 );
											   if(!chkRepeated ) {
												   p1.insertToPageGrid(false, 0 , tableName, colNameValue, ckName, tableTag);
										   }}
										   else {
											   if(z >= (int) p2.min  && z <= (int)p2.max) {
												   boolean chkRepeated = checkRepeatedKeyInt( p2 , (int) ckVal , tableName , 1 );
												   if(!chkRepeated ) {
													   p2.insertToPageGrid(false, 1, tableName, colNameValue, ckName, tableTag);
											   }
												   }
										   }
									   }  
									   }				   
							   else {
							        int i = binaryRR( page , 0 , pagesCount - 1 , ckVal);
//									System.out.println(i + "   size is : " + pagesCount);
									pages iPage;
							        if(i==pagesCount) {
//										System.out.println("dakhal el new page");
										iPage = (pages) page.get(i-1);
									}
									else {
										iPage = (pages) page.get(i);
									}
										if(i == (pagesCount-1))  {
											 iPage.tuples=readPageTable(tableName, (pagesCount-1) );
//											 System.out.println("page size "+ iPage.count );
											 
											 if(iPage.tuples.size() >= n) {
												 pages p =new pages(tableName);
												 page.add(p);
												 pagesCount = pagesCount + 1 ;
												 p.insertToPageGrid( false, (pagesCount-1) , tableName, colNameValue, ckName, tableTag);
											 }else {
												 boolean chkRepeated = checkRepeatedKeyInt( iPage , (int) ckVal , tableName , i );
												   if(!chkRepeated ) {
													   iPage.insertToPageGrid(false, i, tableName, colNameValue, ckName, tableTag);
												   }
											 }
										}
										else {
											 boolean chkRepeated = checkRepeatedKeyInt( iPage , (int) ckVal , tableName , i );
											   if(!chkRepeated ) {
												   iPage.insertToPageGrid( false,i , tableName , colNameValue , ckName , tableTag);
							}}}}


							}
					if(page.size()==3) {
//							System.out.println("page 0 min ,max "+ ((pages)page.get(0)).min + "  " + ((pages)page.get(0)).max +"   size   " +((pages)page.get(0)).count);
//							System.out.println("page 1 min ,max "+ ((pages)page.get(1)).min + "  " + ((pages)page.get(1)).max +"   size   " +((pages)page.get(1)).count);
//							System.out.println("page 2 min ,max "+ ((pages)page.get(2)).min + "  " + ((pages)page.get(2)).max +"   size   " +((pages)page.get(2)).count);
					}		
							}
				} 
			writeTablePages( page ,tableName);
			break;
		case "java.lang.Double":{
			//1st page to be created
			if(pagesCount == 0)
				
			{ 
			  page=new pages(tableName);
		      pages p = new pages(tableName);
		      page.add(p);
		      pagesCount= pagesCount+1;
		      p.insertToPageGrid(false, 0 , tableName, colNameValue, ClustringKeyColoumn, tableTag);
		    }
			//Some pages already exist
			else {

				String ckName = ck(tableName); 
				Object ckVal = getCkValue( colNameValue , ckName );			
				{
					   if (page.size()==1) {
						   Tuples t= new Tuples(colNameValue, ckName, tableTag);
						   Double z = (Double) t.clusterKey;
						   pages p1=(pages) page.get(0);
						   p1.tuples=p1.readPage(tableName, 0);
						   if ((p1).tuples.size()==n && z > ((Double)p1.max) ) {
							   	  pages newPage = new pages(tableName);
							      page.add(newPage);
							     // pagesCount =pagesCount+1;
							      newPage.insertToPageGrid( false,1 ,tableName , colNameValue , ckName , tableTag);
						   }
						   else {
							   pages iPage = (pages) page.get(0);
							   boolean chkRepeated = checkRepeatedKeyDouble( iPage , (double) ckVal , tableName , 0 );
							   if(!chkRepeated ) {
								   iPage.insertToPageGrid( false,0 ,tableName , colNameValue , ckName , tableTag);
						   }}
						   
						   }
					   else {
		   
						   if( pagesCount == 2) {
							   
							   Tuples t= new Tuples(colNameValue, ckName, tableTag);
							   Double z = (Double) t.clusterKey;
							   pages p1=(pages) page.get(0);
							   pages p2=(pages) page.get(1);

							   
							   	if( z > (Double)p2.max) {
								   p2.tuples = readPageTable(tableName, 1);
								   if(p2.count >= n ) {
									    pages ppp = new pages(tableName);
										page.add(ppp);
									    pagesCount= pagesCount+1;
										ppp.insertToPageGrid(false, 2 , tableName, colNameValue, ckName, tableTag);   
								   }
								   else {
									   boolean chkRepeated = checkRepeatedKeyDouble( p2 , (double) ckVal , tableName , 1 );
									   if(!chkRepeated ) {
										   p2.insertToPageGrid(false, 1, tableName, colNameValue, ckName, tableTag);
									   }}
							   	}else {
									   if( z <(Double) p2.min) {
										   boolean chkRepeated = checkRepeatedKeyDouble( p1 , (double) ckVal , tableName , 0 );
										   if(!chkRepeated ) {
											   p1.insertToPageGrid(false, 0 , tableName, colNameValue, ckName, tableTag);
									   }}
									   else {
										   if(z >= (Double) p2.min  && z <= (Double)p2.max) {
											   boolean chkRepeated = checkRepeatedKeyDouble( p2 , (double) ckVal , tableName , 1 );
											   if(!chkRepeated ) {
												   p2.insertToPageGrid(false, 1, tableName, colNameValue, ckName, tableTag);
										   }}
									   }
								   }  
								   }				   
						   else {
						        int i = binaryRR( page , 0 , pagesCount - 1 , ckVal);
//								System.out.println(i + "   size is : " + pagesCount);
								pages iPage;
						        if(i==pagesCount) {
//									System.out.println("dakhal el new page");
									iPage = (pages) page.get(i-1);
								}
								else {
									iPage = (pages) page.get(i);
								}
									if(i == (pagesCount-1))  {
										 iPage.tuples=readPageTable(tableName, (pagesCount-1) );
//										 System.out.println("page size "+ iPage.count );
										 
										 if(iPage.count >= n) {
											 pages p =new pages(tableName);
											 page.add(p);
											 pagesCount = pagesCount + 1 ;
											 p.insertToPageGrid( false, (pagesCount-1) , tableName, colNameValue, ckName, tableTag);
										 }else {
											 iPage.insertToPageGrid(false, i, tableName, colNameValue, ckName, tableTag);
										 
										 }
									}
									else {
										 boolean chkRepeated = checkRepeatedKeyDouble( iPage , (double) ckVal , tableName , i );
										   if(!chkRepeated ) {
											   iPage.insertToPageGrid( false,i , tableName , colNameValue , ckName , tableTag);
						}}}}


						}
				if(page.size()==3) {
//						System.out.println("page 0 min ,max "+ ((pages)page.get(0)).min + "  " + ((pages)page.get(0)).max +"   size   " +((pages)page.get(0)).count);
//						System.out.println("page 1 min ,max "+ ((pages)page.get(1)).min + "  " + ((pages)page.get(1)).max +"   size   " +((pages)page.get(1)).count);
//						System.out.println("page 2 min ,max "+ ((pages)page.get(2)).min + "  " + ((pages)page.get(2)).max +"   size   " +((pages)page.get(2)).count);
				}		
						}
			}
		writeTablePages( page ,tableName);
		break;
		case "java.lang.String":{
			
			//1st page to be created
			if(pagesCount == 0)
				
			{ 
			  page=new pages(tableName);
		      pages p = new pages(tableName);
		      page.add(p);
		      pagesCount= pagesCount+1;
		      p.insertToPageGrid(false, 0 , tableName, colNameValue, ClustringKeyColoumn, tableTag);
		    }
			//Some pages already exist
			else {

				String ckName = ck(tableName); 
				Object ckVal = getCkValue( colNameValue , ckName );			
				{
					   if (page.size()==1) {
						   Tuples t= new Tuples(colNameValue, ckName, tableTag);
						   String z = (String) t.clusterKey;
						   pages p1=(pages) page.get(0);
						   p1.tuples=p1.readPage(tableName, 0);
						   if ((p1).tuples.size()==n && z.compareTo((String)p1.max)>0 ) {
							   	  pages newPage = new pages(tableName);
							      page.add(newPage);
							     // pagesCount =pagesCount+1;
							      newPage.insertToPageGrid( false,1 ,tableName , colNameValue , ckName , tableTag);
						   }
						   else {
							   pages iPage = (pages) page.get(0);
							   boolean chkRepeated = checkRepeatedKeyString( iPage , (String) ckVal , tableName , 0 );
							   if(!chkRepeated ) {
								   iPage.insertToPageGrid( false,0 ,tableName , colNameValue , ckName , tableTag);
						   }}
						   
						   }
					   else {
		   
						   if( pagesCount == 2) {
							   
							   Tuples t= new Tuples(colNameValue, ckName, tableTag);
							   String z = (String) t.clusterKey;
							   pages p1=(pages) page.get(0);
							   pages p2=(pages) page.get(1);

							   
							   	if( z.compareTo((String)p2.max)>0) { 
								   p2.tuples = readPageTable(tableName, 1);
								   if(p2.count >= n ) {
									    pages ppp = new pages(tableName);
										page.add(ppp);
									    pagesCount= pagesCount+1;
										ppp.insertToPageGrid(false, 2 , tableName, colNameValue, ckName, tableTag);   
								   }
								   else {
									   boolean chkRepeated = checkRepeatedKeyString( p2 , (String) ckVal , tableName , 1 );
									   if(!chkRepeated ) {
										   p2.insertToPageGrid(false, 1, tableName, colNameValue, ckName, tableTag);
									   }}
							   	}else {
									   if( z.compareTo((String)p2.max)<0) {
										   boolean chkRepeated = checkRepeatedKeyString( p1 , (String) ckVal , tableName , 0 );
										   if(!chkRepeated ) {
											   p1.insertToPageGrid(false, 0 , tableName, colNameValue, ckName, tableTag);
									   }}
									   else {
										   if(z.compareTo((String)p2.max)<=0 && z.compareTo((String)p2.max)>=0 ) {
											   boolean chkRepeated = checkRepeatedKeyString( p2 , (String) ckVal , tableName , 1 );
											   if(!chkRepeated ) {
												   p2.insertToPageGrid(false, 1, tableName, colNameValue, ckName, tableTag);
										   }}
									   }
								   }  
								   }				   
						   else {
						        int i = binaryRR( page , 0 , pagesCount - 1 , ckVal);
//								System.out.println(i + "   size is : " + pagesCount);
								pages iPage;
						        if(i==pagesCount) {
//									System.out.println("dakhal el new page");
									iPage = (pages) page.get(i-1);
								}
								else {
									iPage = (pages) page.get(i);
								}
									if(i == (pagesCount-1))  {
										 iPage.tuples=readPageTable(tableName, (pagesCount-1) );
//										 System.out.println("page size "+ iPage.count );
										 
										 if(iPage.count >= n) {
											 pages p =new pages(tableName);
											 page.add(p);
											 pagesCount = pagesCount + 1 ;
											 p.insertToPageGrid( false, (pagesCount-1) , tableName, colNameValue, ckName, tableTag);
										 }else {
											 iPage.insertToPageGrid(false, i, tableName, colNameValue, ckName, tableTag);
										 
										 }
									}
									else {
										 boolean chkRepeated = checkRepeatedKeyString( iPage , (String) ckVal , tableName , i );
										   if(!chkRepeated ) {
											   iPage.insertToPageGrid( false,i , tableName , colNameValue , ckName , tableTag);
						}}}}


						}
				if(page.size()==3) {
//						System.out.println("page 0 min ,max "+ ((pages)page.get(0)).min + "  " + ((pages)page.get(0)).max +"   size   " +((pages)page.get(0)).count);
//						System.out.println("page 1 min ,max "+ ((pages)page.get(1)).min + "  " + ((pages)page.get(1)).max +"   size   " +((pages)page.get(1)).count);
//						System.out.println("page 2 min ,max "+ ((pages)page.get(2)).min + "  " + ((pages)page.get(2)).max +"   size   " +((pages)page.get(2)).count);
				}		
						}
		}
		writeTablePages( page ,tableName);
		break;
		case "java.util.Date":{ 
			//1st page to be created
			if(pagesCount == 0)
				
			{ 
			//	System.out.println("Creating the first page");
			  page=new pages(tableName);
		      pages p = new pages(tableName);
		      page.add(p);
		      pagesCount= pagesCount+1;
		      p.insertToPageGrid(false, 0 , tableName, colNameValue, ClustringKeyColoumn, tableTag);
		    }
			//Some pages already exist
			else {

				String ckName = ck(tableName); 
				Object ckVal = getCkValue( colNameValue , ckName );			
				{
					   if (page.size()==1) {
						   
						   Tuples t= new Tuples(colNameValue, ckName, tableTag);
						   Date z = (Date) t.clusterKey;
						   //System.out.println("i am creating ael page w bashoof el value beta3t el z"+z);
						   pages p1=(pages) page.get(0);
						   p1.tuples=p1.readPage(tableName, 0);
						   if ((p1).tuples.size()==n && z.compareTo((Date)p1.max)>0 ) {
							   	  pages newPage = new pages(tableName);
							      page.add(newPage);
							     // pagesCount =pagesCount+1;
							      newPage.insertToPageGrid( false,1 ,tableName , colNameValue , ckName , tableTag);
							     
						   }
						   else {
							//   System.out.println("i am in the first page ");
							   pages iPage = (pages) page.get(0);
							   boolean chkRepeated = checkRepeatedKeyDate( iPage , (Date) ckVal , tableName , 0 );
							   if(!chkRepeated ) {
								   iPage.insertToPageGrid( false,0 ,tableName , colNameValue , ckName , tableTag);
						   }}}
					   else {
		   
						   if( pagesCount == 2) {
							   
							   Tuples t= new Tuples(colNameValue, ckName, tableTag);
							   Date z = (Date) t.clusterKey;
							   pages p1=(pages) page.get(0);
							   pages p2=(pages) page.get(1);

							   
							   	if( z.compareTo((Date)p2.max)>0) { 
								   p2.tuples = readPageTable(tableName, 1);
								   if(p2.count >= n ) {
									    pages ppp = new pages(tableName);
										page.add(ppp);
									    pagesCount= pagesCount+1;
										ppp.insertToPageGrid(false, 2 , tableName, colNameValue, ckName, tableTag);   
								   }
								   else {
									   boolean chkRepeated = checkRepeatedKeyDate( p2 , (Date) ckVal , tableName , 1 );
									   if(!chkRepeated ) {
										   p2.insertToPageGrid(false, 1, tableName, colNameValue, ckName, tableTag);
									   }}
							   	}else {
									   if( z.compareTo((Date)p2.max)<0) {
										   boolean chkRepeated = checkRepeatedKeyDate( p1 , (Date) ckVal , tableName , 0 );
										   if(!chkRepeated ) {
											   p1.insertToPageGrid(false, 0 , tableName, colNameValue, ckName, tableTag);
									   }}
									   else {
										   if(z.compareTo((Date)p2.max)<=0 && z.compareTo((Date)p2.max)>=0 ) {
											   boolean chkRepeated = checkRepeatedKeyDate( p2 , (Date) ckVal , tableName , 1 );
											   if(!chkRepeated ) {
												   p2.insertToPageGrid(false, 1, tableName, colNameValue, ckName, tableTag);
										   }}
									   }
								   }  
								   }				   
						   else {
						        int i = binaryRR( page , 0 , pagesCount - 1 , ckVal);
//								System.out.println(i + "   size is : " + pagesCount);
								pages iPage;
						        if(i==pagesCount) {
//									System.out.println("dakhal el new page");
									iPage = (pages) page.get(i-1);
								}
								else {
									iPage = (pages) page.get(i);
								}
									if(i == (pagesCount-1))  {
										 iPage.tuples=readPageTable(tableName, (pagesCount-1) );
//										 System.out.println("page size "+ iPage.count );
										 
										 if(iPage.count >= n) {
											 pages p =new pages(tableName);
											 page.add(p);
											 pagesCount = pagesCount + 1 ;
											 p.insertToPageGrid( false, (pagesCount-1) , tableName, colNameValue, ckName, tableTag);
										 }else {
											 boolean chkRepeated = checkRepeatedKeyDate( iPage , (Date) ckVal , tableName , i );
											   if(!chkRepeated ) {
												   iPage.insertToPageGrid(false, i, tableName, colNameValue, ckName, tableTag);
											   }
										 }
									}
									else {
										boolean chkRepeated = checkRepeatedKeyDate( iPage , (Date) ckVal , tableName , i );
										   if(!chkRepeated ) {
											   iPage.insertToPageGrid( false,i , tableName , colNameValue , ckName , tableTag);
						}}}}


						}
				if(page.size()==3) {
//						System.out.println("page 0 min ,max "+ ((pages)page.get(0)).min + "  " + ((pages)page.get(0)).max +"   size   " +((pages)page.get(0)).count);
//						System.out.println("page 1 min ,max "+ ((pages)page.get(1)).min + "  " + ((pages)page.get(1)).max +"   size   " +((pages)page.get(1)).count);
//						System.out.println("page 2 min ,max "+ ((pages)page.get(2)).min + "  " + ((pages)page.get(2)).max +"   size   " +((pages)page.get(2)).count);
				}		
						}
			writeTablePages( page ,tableName);
		}break;
		default:break;
		}
		}
	if(this.grids.size()!=0) {
		writeToGrids(gridsName, grids);
	}
		
	}

	public boolean checkRepeatedKeyInt( pages iPage , int ckInt , String tableName , int j) throws ClassNotFoundException, IOException {
//		System.out.println("GOWAAAAAAA");
//		iPage.tuples = iPage.readPage(tableName, j);
//		
//		int location = binarySearchTuples( iPage.tuples , 0, iPage.tuples.size() , (int) ckInt);
//		System.out.println("LOCATION IN ACTUAL PAGE "+ location);
//		if (location != -1) {
//		
//			return true;
//		} 
//		else {
//		
//			int numOF = (iPage.readOverflowVector(tableName)).size();
//			
//			for (int i = 0; i < numOF ; i++) {
//				String name = tableName + "_" + j + "_overflow";
//				Vector overPageVector =iPage.readOverflowVector(tableName);
//				pages overPage = (pages) (overPageVector.get(i));
//				overPage.tuples = overPage.readPage(name, i);
//				
//				int location1 = binarySearchTuples(overPage.tuples, 0, (overPage.tuples.size()) - 1,(int) ckInt);
//				if (location1 != -1) {
//					return true;
//			}}}
		return false;
	} 
		
	public boolean checkRepeatedKeyDouble( pages iPage , double ck , String tableName , int j) throws ClassNotFoundException, IOException {
			
//			iPage.tuples = iPage.readPage(tableName, j);
//			int location = binarySearchTuplesDouble(iPage.tuples, 0, iPage.tuples.size() , ck );
//			if (location != -1) {
//				return true;
//			} else {
//				int numOF = (iPage.readOverflowVector(tableName)).size();
//				for (int i = 0; i < numOF ; i++) {
//					String name = tableName + "_" + j + "_overflow";
//					Vector overPageVector =iPage.readOverflowVector(tableName);
//					pages overPage = (pages) (overPageVector.get(i));
//					overPage.tuples = overPage.readPage(name, i);
//					
//					int location1 = binarySearchTuplesDouble(overPage.tuples, 0, (overPage.tuples.size()) - 1, ck);
//					if (location1 != -1) {
//						return true;
//				}}}
			return false;
		} 
	
	public boolean checkRepeatedKeyString( pages iPage , String ck , String tableName , int j) throws ClassNotFoundException, IOException {
//		
//		iPage.tuples = iPage.readPage(tableName, j);
//		int location = binarySearchTuplesString(iPage.tuples, 0, iPage.tuples.size() , ck );
//		if (location != -1) {
//			return true;
//		} else {
//			int numOF = (iPage.readOverflowVector(tableName)).size();
//			for (int i = 0; i < numOF ; i++) {
//				String name = tableName + "_" + j + "_overflow";
//				Vector overPageVector =iPage.readOverflowVector(tableName);
//				pages overPage = (pages) (overPageVector.get(i));
//				overPage.tuples = overPage.readPage(name, i);
//				
//				int location1 = binarySearchTuplesString(overPage.tuples, 0, (overPage.tuples.size()) - 1, ck);
//				if (location1 != -1) {
//					return true;
//			}}}
		return false;
	} 

	public boolean checkRepeatedKeyDate( pages iPage , Date ck , String tableName , int j) throws ClassNotFoundException, IOException {
//			
//			iPage.tuples = iPage.readPage(tableName, j);
//			int location = binarySearchTuplesDate(iPage.tuples, 0, iPage.tuples.size() , ck );
//			if (location != -1) {
//				return true;
//			} else {
//				int numOF = (iPage.readOverflowVector(tableName)).size();
//				for (int i = 0; i < numOF ; i++) {
//					String name = tableName + "_" + j + "_overflow";
//					Vector overPageVector =iPage.readOverflowVector(tableName);
//					pages overPage = (pages) (overPageVector.get(i));
//					overPage.tuples = overPage.readPage(name, i);
//					
//					int location1 = binarySearchTuplesDate(overPage.tuples, 0, (overPage.tuples.size()) - 1, ck);
//					if (location1 != -1) {
//						return true;
//				}}}
			return false;
		} 
	
	public  Object getCkValue(Hashtable<String, Object> input, String pk) {
		ArrayList<Object> tuplesKey = new ArrayList();

		Enumeration<String> values1 = input.keys();
		Enumeration<Object> values2 = input.elements();
		while (values1.hasMoreElements()) {
			String data = values1.nextElement().toString();
			Object data2 = values2.nextElement();
			tuplesKey.add(data);
			tuplesKey.add(data2);
		}
		for (int i = 0; i < tuplesKey.size(); i += 2) {
			String key = (String) tuplesKey.get(i);
			if (key.equals(pk)) {
				return (tuplesKey.get(i + 1));
			}
		}
		return -1;
	}

	public  ArrayList<String> getOrder(String tableName) throws IOException {
		ArrayList<String> tableTag = new ArrayList();
		BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
		String current = "";
		while ((current = br.readLine()) != null) {
			String[] line = current.split(",");
			for (int i = 0; i < line.length; i += 7) {
				String name = ((String) line[i]).replaceAll("\\s", "");
				String input = ((String) line[i + 1]).replaceAll("\\s", "");
				if (name.equals(tableName)) {
					tableTag.add(input);
				}
			}
		}
		return tableTag;
	}

	public  int binaryR(Vector<pages> arr, int l, int r, int x) {
		if (r >= l) {
			int mid = l + (r - l) / 2;
			// found within range
			if ((((int) arr.get(mid).min) <= x && ((int) arr.get(mid).max) >= x)) {
				return mid;
			}
			// between the min of a range and the max of the next range
			if (!(mid + 1 > r)) {
				if ((((int) arr.get(mid).min) <= x) && ((int) arr.get(mid + 1).min) > x) {
					return mid;
				}
			}

			if (((int) arr.get(mid).min) > x) {
				// between the max of a range and the min of the next range
				if (!(mid - 1 < 0)) {
					if ((((int) arr.get(mid - 1).max) < x)) {
						return mid - 1;
					}
				}

				return binaryR(arr, l, mid - 1, x);
			} else {
//				if (((int)arr.get(mid).max) < x ){
//					if( ((int)arr.get(mid+1).min) >= x || arr.get(mid+1)==null) {
//						return mid;
//					}
//					
//				}
				return binaryR(arr, mid + 1, r, x);
			}
		}
		return l;
	}

	public  int binaryRR(Vector<pages> arr, int l, int r, Object y) throws ParseException {

		String type = chkType(y);
		switch (type) {
		case "java.lang.Integer":
			int x = (Integer) y;
			if (r >= l) {
				int mid = l + (r - l) / 2;
				// found within range
				System.out.println("( arr.get(mid).min)" + ( arr.get(mid).min));
				System.out.println("( arr.get(mid).max)" + ( arr.get(mid).max));

				if ((((int) arr.get(mid).min) <= x && ((int) arr.get(mid).max) >= x)) {
					return mid;
				}
				// between the min of a range and the max of the next range
				if (!(mid + 1 > r)) {
					if ((((int) arr.get(mid).min) <= x)) {
						if (mid + 1 >= arr.size()) {
							return mid;
						}
						if (((int) arr.get(mid + 1).min) > x) {
							return mid;
						}

					}
				}

				if (((int) arr.get(mid).min) > x) {
					// between the max of a range and the min of the next range
					if (!(mid - 1 < 0)) {
						if ((((int) arr.get(mid - 1).max) < x)) {
							return mid - 1;
						}
					}

					return binaryRR(arr, l, mid - 1, y);
				} else {
					return binaryRR(arr, mid + 1, r, y);
				}
			}
			break;
		case "java.lang.Double":
			double d = (Double) y;
			if (r >= l) {
				int mid = l + (r - l) / 2;
				// found within range
				if ((((double) arr.get(mid).min) <= d && ((double) arr.get(mid).max) >= d)) {
					return mid;
				}
				// between the min of a range and the max of the next range
				if (!(mid + 1 > r)) {
					if ((((double) arr.get(mid).min) <= d) && ((double) arr.get(mid + 1).min) > d) {
						return mid;
					}
				}

				if (((double) arr.get(mid).min) > d) {
					// between the max of a range and the min of the next range
					if (!(mid - 1 < 0)) {
						if ((((double) arr.get(mid - 1).max) < d)) {
							return mid - 1;
						}
					}

					return binaryRR(arr, l, mid - 1, y);
				} else {
					return binaryRR(arr, mid + 1, r, y);
				}
			}
			break;
		case "java.lang.String":
			String s = (String) y;
			if (r >= l) {
				int mid = l + (r - l) / 2;
				// found within range
				if ((s.compareTo((String) arr.get(mid).min)) >= 0 && (s.compareTo((String) arr.get(mid).min)) <= 0) {
					return mid;
				}
				// between the min of a range and the max of the next range
				if (!(mid + 1 > r)) {
					if ((s.compareTo((String) arr.get(mid).min)) >= 0 && s.compareTo((String) arr.get(mid).min) < 0) {
						return mid;
					}
				}

				if (s.compareTo((String) arr.get(mid).min) < 0) {
					// between the max of a range and the min of the next range
					if (!(mid - 1 < 0)) {
						if (s.compareTo((String) arr.get(mid).min) > 0) {
							return mid - 1;
						}
					}

					return binaryRR(arr, l, mid - 1, y);
				} else {
					return binaryRR(arr, mid + 1, r, y);
				}
			}
			break;
		case "java.util.Date":
			Date t = (Date) y;
			if (r >= l) {
				int mid = l + (r - l) / 2;
				// found within range
				if ((t.compareTo((Date) arr.get(mid).min)) >= 0 && (t.compareTo((Date) arr.get(mid).min)) <= 0) {
					return mid;
				}
				// between the min of a range and the max of the next range
				if (!(mid + 1 > r)) {
					if ((t.compareTo((Date) arr.get(mid).min)) >= 0 && t.compareTo((Date) arr.get(mid).min) < 0) {
						return mid;
					}
				}

				if (t.compareTo((Date) arr.get(mid).min) < 0) {
					// between the max of a range and the min of the next range
					if (!(mid - 1 < 0)) {
						if (t.compareTo((Date) arr.get(mid).min) > 0) {
							return mid - 1;
						}
					}

					return binaryRR(arr, l, mid - 1, y);
				} else {
					return binaryRR(arr, mid + 1, r, y);
				}
			}
			break;
		default:
			return 0;
		}
		return l;
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

	public  void addOverflow(int i, pages iPage, pages overflow, String tableName, Hashtable<String, Object> colNameValue, ArrayList<String> tableTag)
			throws FileNotFoundException, ParseException, IOException, ClassNotFoundException {
		(iPage.overflow).add(overflow);
		String x = tableName + "_overflow";
		overflow.insertToPage(false, i, tableName, colNameValue, ClustringKeyColoumn, tableTag);
		String fileName = "src/main/resources/data/" + tableName + "_overflow_" + (page.size() - 1) + ".pages";
		try (FileOutputStream fos = new FileOutputStream(fileName, true)) {

		}
	}

	public String ck(String tableName) throws IOException {
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
						ck = ((String) line[i + 1]).replaceAll("\\s", "");
						return ck;
					}
				}
			}
		}

		return ck;

	}

	public  int binarySearch(Vector<pages> a, int l, int r, Object x) {
		// law el clustering key dh integer
		int y = (Integer) x;
		int index = Integer.MAX_VALUE;

		while (l <= r) {
			int mid = (l + r) / 2;

			if (((int) a.get(mid).min) <= y && ((int) a.get(mid).max) >= y) {
				return mid;
			} else if (((int) a.get(mid).max) < y & ((int) a.get(mid + 1).min) > y) {
				return mid + 1;
			} else if (((int) a.get(mid).max) < y) {
				l = mid + 1;
			} else if (((int) a.get(mid).min) > y) {
				r = mid - 1;
			}

		}

		return index;
	}

	public  Vector readPageTable(String tableName, int pageN) throws IOException, ClassNotFoundException {
		String fileName = "src/main/resources/data/" + tableName + "_" + pageN + ".pages";
		Vector<Tuples> tt = new Vector<Tuples>();
		File file = new File(fileName);
		if (file.exists()) {
			FileInputStream fileStream = new FileInputStream(fileName);
			ObjectInputStream is = new ObjectInputStream(fileStream);
			tt = (Vector) is.readObject();
			return tt;
		}
		return tt;
	}

	public Object getckfordelete(Hashtable<String, Object> input, String pk) {
		ArrayList<Object> tuplesKey = new ArrayList();

		Enumeration<String> values1 = input.keys();
		Enumeration<Object> values2 = input.elements();
		while (values1.hasMoreElements()) {
			String data = values1.nextElement().toString();
			Object data2 = values2.nextElement();
			tuplesKey.add(data);
			tuplesKey.add(data2);
		}
		for (int i = 0; i < tuplesKey.size(); i += 2) {
			String key = (String) tuplesKey.get(i);
			if (key.equals(pk)) {
				return (tuplesKey.get(i + 1));
			}
		}
		return null;
	}
   
	public void removeEmptyOverflow(String tableName,int y,Vector pages,int x) {
		overflow p= (overflow) pages.get(x);
        System.out.println("before el if");
    	if (p.size()==0) {
    		for (int i=x;i<pages.size();i++) {
    			int temp=i+1;
    			String oldname = tableName+"_"+y+"_overflow_"+temp+".pages";
    			String newname = tableName+"_"+y+"_overflow_"+i+".pages";
    			File f1 = new File("src/main/resources/data/"+oldname);
    			File f2 = new File("src/main/resources/data/"+newname);
    			f1.renameTo(f2);
                
    		}
    		String delname = tableName+"_"+y+"_overflow_"+(pages.size()-1)+".pages";
    		File f3 = new File("src/main/resources/data/"+delname);
    		f3.delete();
    		pages.remove(x);
    	}
	}
	
	public void removeEmptyPages(String tableName,Vector pages,int x) throws ClassNotFoundException, IOException {
		
    	pages p= (pages) pages.get(x);
        System.out.println("before el if");
    	if (p.size()==0) {
    		p.overflow=p.readOverflowVector("pcs");
    		if (p.overflow.size()==0) {
    		for (int i=x;i<pages.size();i++) {
    			int temp=i+1;
    			String oldname = tableName+"_0"+temp+".pages";
    			String newname = tableName+"_0"+i+".pages";
    			File f1 = new File("src/main/resources/data/"+oldname);
    			File f2 = new File("src/main/resources/data/"+newname);
    			f1.renameTo(f2);
                
    		}
    		String delname = tableName+"_0"+(pages.size()-1)+".pages";
    		File f3 = new File("src/main/resources/data/"+delname);
    		f3.delete();
    		pages.remove(x);
    	  } 
    		}
    	

    }
	
    public void deleteFromT(String tableName, Hashtable<String, Object> columnNameValue)throws IOException, ParseException, ClassNotFoundException, DBAppException {
    	String gridsName = tableName+"_grids";
		this.grids = readGrids(gridsName);
		
    	String ckName = ck(tableName);
		ArrayList<String> tableTag = getOrder(tableName);
		Object ckVal = getckfordelete(columnNameValue, ckName);
		Enumeration<String> values1 = columnNameValue.keys();
		Enumeration<Object> values2 = columnNameValue.elements();
		ArrayList input = new ArrayList<>();
		this.page = readTablePages(tableName);
		ArrayList inputcol = new ArrayList<>();
		while (values1.hasMoreElements()) {
			String data = values1.nextElement().toString();
			Object data2 = values2.nextElement();
			input.add(data);
			inputcol.add(data);
			input.add(data2);

		}
		
		boolean gegoflag=true;
		
        if (this.grids.size()!=0) {
	           int index=0; //location of grid index
		       int common=0; // number of rows in common
		       int count=0;
		        for (int i =0;i<this.grids.size();i++) {
		        	String[] col=this.grids.get(i).columnNames;
		        	for (int j =0;j<col.length;j++) {
		        		for (int k=0;k<inputcol.size();k++) {
		        			if (col[j].equals(inputcol.get(k))) {
		        				count++;
		        			}
		        		}
		        	}
		        	if (count>common) {
		        		common=count;
		        		count=0;
		        		index=i;
		        	}
		        }
		        if (common!=0) 
		        	(this.grids).get(index).deletefromGrid(tableName, input ,index,tableTag);
		        else 
		        	gegoflag=false;
     }
        if (!gegoflag){
        	String pageName="";
        	Tuples t=null;
		System.out.println(input);
		boolean flag = true;
		if (ckVal != null) {
			int x = page.size();
			if (x == 0) {
		//		throw new DBAppException();
			}
			else {
		String dataType = readCsvCKDataType(tableName);
		
		//swicth on pk data type if I am provided with it
		switch(dataType) {				
		case "java.lang.Integer" :	
					boolean found = false;
					for (int j = 0; j < page.size(); j++) {
						pages iPage = (pages) (page.get(j));
						iPage.tuples = iPage.readPage(tableName, j);
						//////////////////////////////////////////////////  iPage.tuples.size() - 1
						int location = binarySearchTuples( iPage.tuples , 0 , iPage.tuples.size() , (int) ckVal);
						System.out.println("LOCATION " + location);
						
						if (location != -1) {
							boolean deleteFlag = chkdeletion(input,  (iPage.tuples.get(location)), tableName);
							if(deleteFlag) {
								t=(iPage.tuples.get(location));
								pageName= tableName+"_"+j;
								if (this.grids.size()!=0) {
									this.grids.get(0).deletefromALLGrids(tableName, 100000000, t, pageName);
								}
								System.out.println("deleted from actual page");
								iPage.tuples.remove(location);
								//here
								iPage.count = iPage.tuples.size() ;
								iPage.writeTupleToPage(tableName, j, iPage.tuples);
								found = true;
								removeEmptyPages(tableName,page,j);
							}
						} else {
							int numOF = (iPage.readOverflowVector(tableName)).size();
							for ( int i = 0 ; i < numOF ; i++ ) {
								String name = tableName + "_" + j + "_overflow" ;
								//Vector overPageVector = ((pages) page.get(j)).overflow;
								Vector overPageVector = iPage.readOverflowVector(tableName) ;

								pages overPage = (pages) (overPageVector.get(i));
								overPage.tuples = overPage.readPage(name, i);
								int location1 = binarySearchTuples(overPage.tuples, 0, overPage.tuples.size() - 1 ,(int) ckVal);
								
								if (location1 != -1) {
									boolean deleteFlag = chkdeletion(input,  (overPage.tuples.get(location1)), tableName);
									if(deleteFlag) {
										t=(overPage.tuples.get(location1));
										pageName= tableName+"_"+j+"_overflow_"+i;
										if (this.grids.size()!=0) {
											this.grids.get(0).deletefromALLGrids(tableName, 100000000, t, pageName);
										}
										System.out.println("deleted from overflow page");
										overPage.tuples.remove(location1);
										overPage.writeTupleToPage(name, i, overPage.tuples);
										found = true;
										overPage.count = overPage.tuples.size() ;
										removeEmptyOverflow(tableName,j,iPage.readOverflowVector(tableName),i);
										break;
								}}
							}
						}
						if (found) {
							break;
						} }
					if (!found) {
						System.out.println("NOT FOUND FOOOO2");
	//					throw new DBAppException();
					}
			break;
		case "java.lang.Double"  :
					boolean found1 = false;
					for (int j = 0; j < page.size(); j++) {
						pages iPage = (pages) (page.get(j));
						iPage.tuples = iPage.readPage(tableName, j);
						//////////////////////////////////////////////////  iPage.tuples.size() - 1
						int location = binarySearchTuplesDouble( iPage.tuples , 0 , iPage.tuples.size() , (double) ckVal);
						System.out.println("LOCATION " + location);
						
						if (location != -1) {
							boolean deleteFlag = chkdeletion(input,  (iPage.tuples.get(location)), tableName);
							if(deleteFlag) {
								t=(iPage.tuples.get(location));
								pageName= tableName+"_"+j;
								if (this.grids.size()!=0) {
									this.grids.get(0).deletefromALLGrids(tableName, 100000000, t, pageName);
								}
								System.out.println("deleted from actual page");
								iPage.tuples.remove(location);
								//here
								iPage.count = iPage.tuples.size() ;
								iPage.writeTupleToPage(tableName, j, iPage.tuples);
								found1 = true;
								removeEmptyPages(tableName,page,j);
							}
						} else {
							int numOF = (iPage.readOverflowVector(tableName)).size();
							for ( int i = 0 ; i < numOF ; i++ ) {
								String name = tableName + "_" + j + "_overflow" ;
								//Vector overPageVector = ((pages) page.get(j)).overflow;
								Vector overPageVector = iPage.readOverflowVector(tableName) ;
		
								pages overPage = (pages) (overPageVector.get(i));
								overPage.tuples = overPage.readPage(name, i);
								int location1 = binarySearchTuplesDouble(overPage.tuples, 0, overPage.tuples.size() - 1 ,(double) ckVal);
								
								if (location1 != -1) {
									boolean deleteFlag = chkdeletion(input,  (overPage.tuples.get(location1)), tableName);
									if(deleteFlag) {
										t=(overPage.tuples.get(location1));
										pageName= tableName+"_"+j+"_overflow_"+i;
										if (this.grids.size()!=0) {
											this.grids.get(0).deletefromALLGrids(tableName, 100000000, t, pageName);
										}
										System.out.println("deleted from overflow page");
										overPage.tuples.remove(location1);
										overPage.writeTupleToPage(name, i, overPage.tuples);
										found1 = true;
										overPage.count = overPage.tuples.size() ;
										removeEmptyOverflow(tableName,j,iPage.readOverflowVector(tableName),i);
										break;
								}}
							}
						}
						if (found1) {
							break;
						}
					}
					if (!found1) {
						System.out.println("NOT FOUND FOOOO2");
		//					throw new DBAppException();
					}
			break;
		
			
		case "java.lang.String" :
					boolean found2 = false;
					for (int j = 0; j < page.size(); j++) {
						pages iPage = (pages) (page.get(j));
						iPage.tuples = iPage.readPage(tableName, j);
						//////////////////////////////////////////////////  iPage.tuples.size() - 1
						int location = binarySearchTuplesString( iPage.tuples , 0 , iPage.tuples.size() , (String) ckVal);
						System.out.println("LOCATION " + location);
						
						if (location != -1) {
							boolean deleteFlag = chkdeletion(input,  (iPage.tuples.get(location)), tableName);
							if(deleteFlag) {
								t=(iPage.tuples.get(location));
								pageName= tableName+"_"+j;
								if (this.grids.size()!=0) {
									this.grids.get(0).deletefromALLGrids(tableName, 100000000, t, pageName);
								}
								System.out.println("deleted from actual page");
								iPage.tuples.remove(location);
								//here
								iPage.count = iPage.tuples.size() ;
								iPage.writeTupleToPage(tableName, j, iPage.tuples);
								found2 = true;
								removeEmptyPages(tableName,page,j);
							}
						} else {
							int numOF = (iPage.readOverflowVector(tableName)).size();
							for ( int i = 0 ; i < numOF ; i++ ) {
								String name = tableName + "_" + j + "_overflow" ;
								//Vector overPageVector = ((pages) page.get(j)).overflow;
								Vector overPageVector = iPage.readOverflowVector(tableName) ;
		
								pages overPage = (pages) (overPageVector.get(i));
								overPage.tuples = overPage.readPage(name, i);
								int location1 = binarySearchTuplesString(overPage.tuples, 0, overPage.tuples.size() - 1 ,(String) ckVal);
								
								if (location1 != -1) {
									boolean deleteFlag = chkdeletion(input,  (overPage.tuples.get(location1)), tableName);
									if(deleteFlag) {
										t=(overPage.tuples.get(location1));
										pageName= tableName+"_"+j+"_overflow_"+i;
										if (this.grids.size()!=0) {
											this.grids.get(0).deletefromALLGrids(tableName, 100000000, t, pageName);
										}
										System.out.println("deleted from overflow page");
										overPage.tuples.remove(location1);
										overPage.writeTupleToPage(name, i, overPage.tuples);
										found2 = true;
										overPage.count = overPage.tuples.size() ;
										removeEmptyOverflow(tableName,j,iPage.readOverflowVector(tableName),i);
										break;
								}}
							}
						}
						if (found2) {
							break;
						}
					}
					if (!found2) {
						System.out.println("NOT FOUND FOOOO2");
		//					throw new DBAppException();
					}
			break;
		case "java.util.Date" :
//						System.out.println(ckVal);
//						SimpleDateFormat sdf = new SimpleDateFormat("EEE MM dd HH:mm:ss z yyyy", Locale.US);
//						Date ckDate = sdf.parse(ckVal.toString());

						boolean found3 = false;
						for (int j = 0; j < page.size(); j++) {
							pages iPage = (pages) (page.get(j));
							iPage.tuples = iPage.readPage(tableName, j);
							System.out.println("BEFORE BINARY SEARCH");
							//////////////////////////////////////////////////  iPage.tuples.size() - 1
							int location = binarySearchTuplesDate( iPage.tuples , 0 , iPage.tuples.size() , (Date)ckVal);
							System.out.println("LOCATION " + location);
							
							if (location != -1) {
								boolean deleteFlag = chkdeletion(input,  (iPage.tuples.get(location)), tableName);
								if(deleteFlag) {
									t=(iPage.tuples.get(location));
									pageName= tableName+"_"+j;
									if (this.grids.size()!=0) {
										this.grids.get(0).deletefromALLGrids(tableName, 100000000, t, pageName);
									}
									System.out.println("deleted from actual page");
									iPage.tuples.remove(location);
									//here
									iPage.count = iPage.tuples.size() ;
									iPage.writeTupleToPage(tableName, j, iPage.tuples);
									found3 = true;
									removeEmptyPages(tableName,page,j);
								}
							} else {
								int numOF = (iPage.readOverflowVector(tableName)).size();
								for ( int i = 0 ; i < numOF ; i++ ) {
									String name = tableName + "_" + j + "_overflow" ;
									//Vector overPageVector = ((pages) page.get(j)).overflow;
									Vector overPageVector = iPage.readOverflowVector(tableName) ;
			
									pages overPage = (pages) (overPageVector.get(i));
									overPage.tuples = overPage.readPage(name, i);
									int location1 = binarySearchTuplesDate(overPage.tuples, 0, overPage.tuples.size() - 1 ,(Date)ckVal);
									
									if (location1 != -1) {
										boolean deleteFlag = chkdeletion(input,  (overPage.tuples.get(location1)), tableName);
										if(deleteFlag) {
											t=(overPage.tuples.get(location1));
											pageName= tableName+"_"+j+"_overflow_"+i;
											if (this.grids.size()!=0) {
												this.grids.get(0).deletefromALLGrids(tableName, 100000000, t, pageName);
											}
											System.out.println("deleted from overflow page");
											overPage.tuples.remove(location1);
											overPage.writeTupleToPage(name, i, overPage.tuples);
											found3 = true;
											overPage.count = overPage.tuples.size() ;
											removeEmptyOverflow(tableName,j,iPage.readOverflowVector(tableName),i);
											break;
									}}
								}
							}
								if (found3) {
									break;
								}
							}
							if (!found3) {
								System.out.println("NOT FOUND FOOOO2");
				//					throw new DBAppException();
							}
					break;		
		}	}	}
					else {
						boolean found =false;
						for (int j = 0; j < page.size(); j++) {
							pages iPage = (pages) (page.get(j));
							iPage.tuples = iPage.readPage(tableName, j);
							//LOOP OVER ACTUAL PAGE'S TUPLES
							for( int tp = 0 ; tp < iPage.tuples.size() ; tp ++  ) {
								boolean deleteFlag = chkdeletion(input,  (iPage.tuples.get(tp)), tableName);
								if( deleteFlag ) {
									t=((iPage.tuples.get(tp)));
									pageName= tableName+"_"+j;
									if (this.grids.size()!=0) {
										this.grids.get(0).deletefromALLGrids(tableName, 100000000, t, pageName);
									}
									iPage.tuples.remove(tp);
									iPage.count = iPage.count - 1;
									iPage.writeTupleToPage(tableName, j, iPage.tuples);
									found = true;
									removeEmptyPages(tableName,page,j);
								}
							}
							//LOOP OVER Overflow PAGES
							for (int i = 0; i < iPage.numOverflow; i++) {
									String name = tableName + "_" + j + "_overflow";
									Vector overPageVector = ((pages) page.get(j)).overflow;
									pages overPage = (pages) (overPageVector.get(i));
									overPage.tuples = overPage.readPage(name, i);
									
									for( int op = 0; op < overPage.tuples.size() ; op ++ ) {
										boolean deleteFlag = chkdeletion(input,  (overPage.tuples.get(op)), tableName);
										if( deleteFlag ) {
											t=(overPage.tuples.get(op));
											pageName= tableName+"_"+j+"_overflow_"+i;
											if (this.grids.size()!=0) {
												this.grids.get(0).deletefromALLGrids(tableName, 100000000, t, pageName);
											}
											overPage.tuples.remove(op);
											overPage.count = overPage.count - 1;
											overPage.writeTupleToPage(name, i, overPage.tuples);
											found = true;
											removeEmptyOverflow(tableName,j,iPage.readOverflowVector(tableName),i);
										}  }  } }
						if (!found) {
							System.out.println("NOT FOUND HEREEEE");
							//throw new DBAppException();
			}	} 
			
    } 
        if(this.grids.size()!=0) {
			writeToGrids(gridsName, grids);
		}
    }

	public  void updateT(String tableName, String clusteringKeyValue ,  Hashtable<String, Object> columnNameValue) throws IOException, DBAppException, ClassNotFoundException, ParseException {
		String gridsName = tableName+"_grids";
		this.grids = readGrids(gridsName);
		Tuples oldt=null;
		Tuples newt=null;
		String pageName="";
		ArrayList<String> tableTag = getOrder(tableName);

		
		String dataType = readCsvCKDataType(tableName);
		Enumeration<String> values1 = columnNameValue.keys();
		Enumeration<Object> values2 = columnNameValue.elements();
		ArrayList input = new ArrayList<>();
		ArrayList inputcol = new ArrayList<>();
		this.page = readTablePages(tableName);
		String ckName = ck(tableName);
		while (values1.hasMoreElements()) {
			String data = values1.nextElement().toString();
			Object data2 = values2.nextElement();
			input.add(data);
			inputcol.add(data);
			input.add(data2);

		}
		
		boolean gegoflag=false;
		boolean gridup=false;	
		        if (this.grids.size()!=0) {
			           int index=0; //location of grid index
				     
				        for (int i =0;i<this.grids.size();i++) {
				        	String[] col=this.grids.get(i).columnNames;
				        	for (int j =0;j<col.length;j++) {
				        		if (col[j].equals(ckName)) {
				        			gridup=(this.grids).get(index).updateGrid(tableName, input ,i,tableTag,ckName,clusteringKeyValue,this.grids);
				        			gegoflag=true;
				        			}}} }
		        if (gegoflag) {
		        	
		        }
		        else {
		switch(dataType) {
		
		case "java.lang.Integer" :
			int ckInt = Integer.parseInt(clusteringKeyValue);
			int x = page.size();
			//int x = pagesCount + 1;
			if (x == 0) {
				/////////////////////////////////////////////////////////////////////////////////////////////////should I throw it?
			//	throw new DBAppException();
			} else {
					    boolean found = false;
					    int j = binaryRR( page , 0 , x-1, ckInt);
					 //   System.out.println("page size "+ x);
						pages iPage = (pages) (page.get(j));
						iPage.tuples = iPage.readPage(tableName, j);
						int location = binarySearchTuples(iPage.tuples, 0, iPage.tuples.size() , (int) ckInt);
						System.out.println("LOCATION " + location);
						
						if (location != -1) {
								Tuples tup = (Tuples)(iPage.tuples.get( location )); /// tup old tuple
								oldt=tup;
							    for(int y = 0 ; y < input.size() ; y+=2 ) {
							    	String inKey= (input.get(y)).toString();
							    	for( int z = 0 ; z < tableTag.size() ; z++ ) {
								    	String tableKey= (tableTag.get(z)).toString();
							    		if( inKey.equals(tableKey) ) {
							    			  tup.tuples[z] = (input.get(y+1));
							    			  iPage.tuples= iPage.readPage( tableName , j );
											  Vector<Tuples> temp = iPage.readPage( tableName , j );
											  temp.remove(location);
											  temp.add(tup);
											  newt=tup;
											  iPage.writeTupleToPage(tableName, j, temp );
											  pageName=tableName+"_"+j;
											  found = true;
							    		}
							    	}
							    }
							
						} else {
							int numOF = (iPage.readOverflowVector(tableName)).size();
							
							for (int i = 0; i < numOF ; i++) {
								String name = tableName + "_" + j + "_overflow";
								//Vector overPageVector = ((pages) page.get(j)).overflow;
								
								Vector overPageVector =iPage.readOverflowVector(tableName);
								
								pages overPage = (pages) (overPageVector.get(i));
								overPage.tuples = overPage.readPage(name, i);
								
								int location1 = binarySearchTuples(overPage.tuples, 0, (overPage.tuples.size()) - 1,(int) ckInt);
								
								if (location1 != -1) {
									Tuples tup = (Tuples)(overPage.tuples.get( location1 ));
									oldt=tup;
								    for(int y = 0 ; y < input.size() ; y+=2 ) {
								    	String inKey= (input.get(y)).toString();
								    	for( int z = 0 ; z < tableTag.size() ; z++ ) {
									    	String tableKey= (tableTag.get(z)).toString();
								    		if( inKey.equals(tableKey) ) {
								    			tup.tuples[z] = (input.get(y+1));
								    			overPage.tuples= overPage.readPage( name , j );
											    Vector<Tuples> temp = overPage.readPage( name , j );
												//overPage.tuples.remove(location1);
												temp.remove(location1);
												temp.add(tup);
												newt=tup;
												overPage.writeTupleToPage(name, j, temp );
												pageName=name+"_"+i;
												found = true;
								    		} } } } }  }
						if (found) {
							break;
					}if (!found) {
						System.out.println("NOT FOUND");
				//		throw new DBAppException();		
			  }}
			  break;
		case "java.lang.Double"  : 
			double ckDoub = Double.parseDouble(clusteringKeyValue);
			int x1 = page.size();
			//int x1 = pagesCount + 1;
			if (x1 == 0) {
				//throw new DBAppException();
			} else {
					boolean found = false;
						
						int j = binaryRR( page , 0 , x1-1, ckDoub);
					//for (int j = 0; j < page.size(); j++) {
						pages iPage = (pages) (page.get(j));
						iPage.tuples = iPage.readPage(tableName, j);
						int location = binarySearchTuplesDouble(iPage.tuples, 0, iPage.tuples.size() , (double) ckDoub);
						System.out.println("LOCATION " + location);
						
						if (location != -1) {
								Tuples tup = (Tuples)(iPage.tuples.get( location ));
								oldt=tup;
							    for(int y = 0 ; y < input.size() ; y+=2 ) {
							    	String inKey= (input.get(y)).toString();
							    	for( int z = 0 ; z < tableTag.size() ; z++ ) {
								    	String tableKey= (tableTag.get(z)).toString();
							    		if( inKey.equals(tableKey) ) {
							    			 tup.tuples[z] = (input.get(y+1));
							    			 iPage.tuples= iPage.readPage( tableName , j );
											 Vector<Tuples> temp = iPage.readPage( tableName , j );
											 //keep the next line?
											 iPage.tuples.remove(location);
											 temp.remove(location);
											 temp.add(tup);
											 newt=tup;
											 iPage.writeTupleToPage(tableName, j, temp );
											 pageName=tableName+"_"+j;
											 found = true;
							    		}
							    	}
							    }
							
						} else {
							int numOF = (iPage.readOverflowVector(tableName)).size();
							for (int i = 0; i < numOF ; i++) {
								String name = tableName + "_" + j + "_overflow";
								//Vector overPageVector = ((pages) page.get(j)).overflow;
								Vector overPageVector =iPage.readOverflowVector(tableName);
								
								pages overPage = (pages) (overPageVector.get(i));
								overPage.tuples = overPage.readPage(name, i);
								int location1 = binarySearchTuplesDouble(overPage.tuples, 0, (overPage.tuples.size()) - 1 , (double) ckDoub);
								if (location1 != -1) {
									Tuples tup = (Tuples)(overPage.tuples.get( location1 ));
									oldt=tup;
								    for(int y = 0 ; y < input.size() ; y+=2 ) {
								    	String inKey= (input.get(y)).toString();
								    	for( int z = 0 ; z < tableTag.size() ; z++ ) {
									    	String tableKey= (tableTag.get(z)).toString();
								    		if( inKey.equals(tableKey) ) {
								    			 tup.tuples[z] = (input.get(y+1));
								    			 overPage.tuples= overPage.readPage( name , j );
								    			 Vector<Tuples> temp = overPage.readPage( name , j );
								    			 overPage.tuples.remove(location1);
								    			 temp.remove(location);
								    			 temp.add(tup);
								    			 newt=tup;
								    			 overPage.writeTupleToPage(name, j, temp );
								    			 pageName=name+"_"+i;
								    			 found = true;
								    		}
								    	}
								    }
								} }  }
						if (found) {
							break;
					}if (!found) {
						System.out.println("NOT FOUND");
					//	throw new DBAppException();		
			  }}
			  break;
		case "java.lang.String" :
			int x2 = page.size();
			//int x2 = pagesCount + 1;
			if (x2 == 0) {
				//throw new DBAppException();
			} else {
					boolean found = false;
				    int j = binaryRR( page , 0 , x2-1 , clusteringKeyValue);
					//for (int j = 0; j < page.size(); j++) {
						pages iPage = (pages) (page.get(j));
						iPage.tuples = iPage.readPage(tableName, j);
						int location = binarySearchTuplesString(iPage.tuples, 0, iPage.tuples.size() , clusteringKeyValue);
						System.out.println("LOCATION " + location);
						
						if (location != -1) {
								Tuples tup = (Tuples)(iPage.tuples.get( location ));
								oldt=tup;
							    for(int y = 0 ; y < input.size() ; y+=2 ) {
							    	String inKey= (input.get(y)).toString();
							    	for( int z = 0 ; z < tableTag.size() ; z++ ) {
								    	String tableKey= (tableTag.get(z)).toString();
							    		if( inKey.equals(tableKey) ) {
							    			tup.tuples[z] = (input.get(y+1));
							    		}
							    	}
							    }
							    iPage.tuples= iPage.readPage( tableName , j );
							    Vector<Tuples> temp = iPage.readPage( tableName , j );
								iPage.tuples.remove(location);
								temp.add(tup);
								newt=tup;
								iPage.writeTupleToPage(tableName, j, temp );
								pageName=tableName+"_"+j;
								found = true;
							
						} else {
							int numOF = (iPage.readOverflowVector(tableName)).size();
							for (int i = 0; i < numOF ; i++) {
								String name = tableName + "_" + j + "_overflow";
								//Vector overPageVector = ((pages) page.get(j)).overflow;
								Vector overPageVector =iPage.readOverflowVector(tableName);
								
								pages overPage = (pages) (overPageVector.get(i));
								overPage.tuples = overPage.readPage(name, i);
								int location1 = binarySearchTuplesString(overPage.tuples, 0, (overPage.tuples.size()) - 1 , clusteringKeyValue );
								if (location1 != -1) {
									Tuples tup = (Tuples)(overPage.tuples.get( location1 ));
									oldt=tup;
								    for(int y = 0 ; y < input.size() ; y+=2 ) {
								    	String inKey= (input.get(y)).toString();
								    	for( int z = 0 ; z < tableTag.size() ; z++ ) {
									    	String tableKey= (tableTag.get(z)).toString();
								    		if( inKey.equals(tableKey) ) {
								    			tup.tuples[z] = (input.get(y+1));
								    		}
								    	}
								    }
								    overPage.tuples= overPage.readPage( name , j );
								    Vector<Tuples> temp = overPage.readPage( name , j );
									overPage.tuples.remove(location1);
									temp.add(tup);
									newt=tup;
									overPage.writeTupleToPage(name, j, temp );
									pageName=name+"_"+i;
									found = true;
								} }  }
						if (found) {
							break;
					}if (!found) {
						System.out.println("NOT FOUND");
					//	throw new DBAppException();		
			  }}
		
			break;
		case "java.util.Date" :
			
//			Date ckDate = new SimpleDateFormat("yyyy-MM-DD").parse(clusteringKeyValue);
//			DateFormat format = new SimpleDateFormat("yyyy-MM-DD", Locale.ENGLISH);
//			Date ckDate = format.parse(clusteringKeyValue);
			
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date ckDate = sdf.parse(clusteringKeyValue);
		
		//	SimpleDateFormat date1 = new SimpleDateFormat("yyyy-MM-DD");
		//	String ckDate = date1.format(parsedDate);
			
			int x3 = page.size();
			//int x3 = pagesCount + 1;
			if (x3 == 0) {
			//	throw new DBAppException();
			} else {
					boolean found = false;
					    int j = binaryRR( page , 0 , x3-1 , ckDate);
				//	    System.out.println("page size "+ page.size());
					    //for (int j = 0; j < page.size(); j++) {
						pages iPage = (pages) (page.get(j));
						iPage.tuples = iPage.readPage(tableName, j);
						int location = binarySearchTuplesDate(iPage.tuples, 0, iPage.tuples.size() , ckDate);
					//	System.out.println("LOCATION " + location);
						
						if (location != -1) {
								Tuples tup = (Tuples)(iPage.tuples.get( location ));
								oldt=tup;
							    for(int y = 0 ; y < input.size() ; y+=2 ) {
							    	String inKey= (input.get(y)).toString();
							    	for( int z = 0 ; z < tableTag.size() ; z++ ) {
								    	String tableKey= (tableTag.get(z)).toString();
							    		if( inKey.equals(tableKey) ) {
							    			  tup.tuples[z] = (input.get(y+1));
							    			  iPage.tuples= iPage.readPage( tableName , j );
											  Vector<Tuples> temp = iPage.readPage( tableName , j );
											  temp.remove(location);
											  temp.add(tup);
											  newt=tup;
											  iPage.writeTupleToPage(tableName, j, temp );
											  pageName=tableName+"_"+j;
												found = true;
							    		}
							    	}
							    }
							
						} else {
							int numOF = (iPage.readOverflowVector(tableName)).size();
							for (int i = 0; i < numOF ; i++) {
								String name = tableName + "_" + j + "_overflow";
								//Vector overPageVector = ((pages) page.get(j)).overflow;
								Vector overPageVector =iPage.readOverflowVector(tableName);
								pages overPage = (pages) (overPageVector.get(i));
								overPage.tuples = overPage.readPage(name, i);
								int location1 = binarySearchTuplesDate(overPage.tuples, 0, (overPage.tuples.size()) - 1, ckDate);
								if (location1 != -1) {
									Tuples tup = (Tuples)(overPage.tuples.get( location1 ));
									oldt=tup;
								    for(int y = 0 ; y < input.size() ; y+=2 ) {
								    	String inKey= (input.get(y)).toString();
								    	for( int z = 0 ; z < tableTag.size() ; z++ ) {
									    	String tableKey= (tableTag.get(z)).toString();
								    		if( inKey.equals(tableKey) ) {
								    			tup.tuples[z] = (input.get(y+1));
								    			overPage.tuples= overPage.readPage( name , j );
											    Vector<Tuples> temp = overPage.readPage( name , j );
												//overPage.tuples.remove(location1);
												temp.remove(location1);
												temp.add(tup);
												newt=tup;
												overPage.writeTupleToPage(name, j, temp );
												pageName=name+"_"+i;
												found = true;
								    		}
								    	}
								    }
								} }  }
						if (found) {
							if(this.grids.size()!=0) 
							    this.grids.get(0).updateALLGrids(tableName,  pageName ,this.grids,newt,oldt);break;
					}if (!found) {
						System.out.println("NOT FOUND");
				//		throw new DBAppException();		
			  }}
			  break;
		default: 
			//throw new DBAppException();	
			break;
		}
		
		}
		        if(this.grids.size()!=0) {
					writeToGrids(gridsName, grids);
				}
		        }
	
    public  String readCsvCKDataType(String tablename) throws IOException{
		
		BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
		String current = "";
		ArrayList con = new ArrayList();
		while ((current =br.readLine())!= null) {
			String[] line = current.split(",");
			for(int i=0 ;i<line.length;i+=7) {
				
				if(line[i].equals(tablename)) {
					ArrayList con2 = new ArrayList();
					for (int j = i ; j < 7 ; j++ ) {
						con.add(line[j] );
						//find the clustring key
						String x = (line[i+3].toString()).replaceAll("\\s", "");
						if(x.equalsIgnoreCase("True")) {
							return (line[i+2].toString()).replaceAll("\\s", "");
							}
						}
					}
				}
			}
		return "";
		
	}
	
	public  int binarySearchTuples(Vector<Tuples> arr, int l, int r, int x) {
		if (r >= l) {
			int mid = l + (r - l) / 2;

			if (mid >= arr.size() - 1) {
				return arr.size() - 1;
			}

			System.out.println("middle point " + mid);
			// If the element is present at the middle itself
			if ((int) (arr.get(mid).clusterKey) == x)
				return mid;

			// If element is smaller than mid, then it can only be present in left subarray
			if ((int) (arr.get(mid).clusterKey) > x)
				return binarySearchTuples(arr, l, mid - 1, x);

			// Else the element can only be present in right subarray
			return binarySearchTuples(arr, mid + 1, r, x);
		}

		// We reach here when element is not
		// present in array
		return -1;
	}
	
	public int binarySearchTuplesDouble(Vector<Tuples> arr, int l, int r, double x) {
		if (r >= l) {
			int mid = l + (r - l) / 2;

			if (mid >= arr.size() - 1) {
				return arr.size() - 1;
			}

			System.out.println("middle point " + mid);
			// If the element is present at the middle itself
			if ((double) (arr.get(mid).clusterKey) == x)
				return mid;

			// If element is smaller than mid, then it can only be present in left subarray
			if ((double) (arr.get(mid).clusterKey) > x)
				return binarySearchTuplesDouble(arr, l, mid - 1, x);

			// Else the element can only be present in right subarray
			return binarySearchTuplesDouble(arr, mid + 1, r, x);
		}

		// We reach here when element is not
		// present in array
		return -1;
	}

	public  int binarySearchTuplesString(Vector<Tuples> arr, int l, int r, String x) {
		if (r >= l) {
			int mid = l + (r - l) / 2;

			if (mid >= arr.size() - 1) {
				return arr.size() - 1;
			}

			System.out.println("middle point " + mid);
			// If the element is present at the middle itself
			if (((String) (arr.get(mid).clusterKey)).equals(x))
				return mid;

			// If element is smaller than mid, then it can only be present in left subarray
			if (((String) (arr.get(mid).clusterKey)).compareTo(x) > 0 )
				return binarySearchTuplesString(arr, l, mid - 1, x);

			// Else the element can only be present in right subarray
			return binarySearchTuplesString(arr, mid + 1, r, x);
		}

		// We reach here when element is not
		// present in array
		return -1;
	}

	public  int binarySearchTuplesDate(Vector<Tuples> arr, int l, int r, Date x) {
		if (r >= l) {
			int mid = l + (r - l) / 2;

			if (mid >= arr.size() - 1) {
				return arr.size() - 1;
			}

			System.out.println("middle point " + mid);
			// If the element is present at the middle itself
			if ((((Date) (arr.get(mid).clusterKey)).compareTo(x)) == 0)
				return mid;

			// If element is smaller than mid, then it can only be present in left subarray
			if (((Date) (arr.get(mid).clusterKey)).compareTo(x) > 0 )
				return binarySearchTuplesDate(arr, l, mid - 1, x);

			// Else the element can only be present in right subarray
			return binarySearchTuplesDate(arr, mid + 1, r, x);
		}

		// We reach here when element is not
		// present in array
		return -1;
	}
	
	public  boolean chkdeletion(ArrayList input, Tuples tuple, String tableName)
			throws IOException, ParseException, DBAppException {
		if (input.isEmpty()) {
			return false;
		}
		boolean flag = true;
		ArrayList<String> tag = getOrder(tableName);

		for (int i = 0; i < input.size(); i += 2) {
			String r = (String) input.get(i);
			boolean found = false;
			if (!(tag.contains(r))) {
				throw new DBAppException();
			}

			for (int j = 0; j < tag.size(); j++) {
				if (r.equals(tag.get(j))) {
					found = true;
					Object inputVal = input.get(i + 1);
					Object tupleVal = tuple.tuples[j];
					String type = (inputVal.getClass()).toString();
					if (type.substring(16).equals("Integer")) {
						if (((int) inputVal != (int) tupleVal)) {
							flag = false;
						}
					}
					if (type.substring(16).equals("Double")) {
						if (!((Double) inputVal).equals((Double) tupleVal)) {
							flag = false;
						}
					}
					if (type.substring(16).equals("String")) {
						if (!(((String) inputVal).compareTo((String) tupleVal) == 0)) {
							flag = false;
						}
					}
					int cmin = 0;
			        int cmax = 0;
					if (type.substring(16).equals("Date")) {
						if((((Date)inputVal).compareTo((Date)tupleVal)) != 0 ) {
							flag =false;
						}
					}
					if (!flag) {
						return false;
					}

				}
				if (found == true) {
					break;
				}

			}

		}
		return true;
	}
		
	public  boolean isValidFormat( String value ) {
	        Date date = null;
	        String format = "YYYY-MM-DD" ;
	        try {
	            SimpleDateFormat sdf = new SimpleDateFormat(format);
	            date = sdf.parse(value);
	            if (!value.equals(sdf.format(date))) {
	                date = null;
	            }
	        } catch (ParseException ex) {
	            ex.printStackTrace();
	        }
	        return date != null ;
	    }
	
	 
	 public  void writeToGrids(String Name , Vector<Grid> grids) throws FileNotFoundException, IOException {
			String fileName = "src/main/resources/data/" + Name+ ".grids";
		    try {
		    	ObjectOutputStream os =new ObjectOutputStream(new FileOutputStream(fileName) );
		    	os.writeObject(grids);
		    	os.close();	
			} catch (Exception e) {
				// TODO: handle exception
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
	 
	 
	public static void main(String[] args) throws IOException, ParseException, ClassNotFoundException, DBAppException {

		//String oldname = tableName+"_"+i+1+".pages";
		//String newname=tableName+"_"+i+".pages";
		
		   
		
//		Vector p =new Vector<Integer>();
//		
//		p.add(0);
//		p.add(1);
//		p.add(2);
//		p.add(3);
//		p.add(4);
//		for (int i =0;i<p.size();i++) {
//			System.out.println("In index "+i+"     "+(Integer)p.get(i));
//		}
//		p.remove(2);
//		System.out.println("-------------------------------");
//		for (int i =0;i<p.size();i++) {
//			System.out.println("In index "+i+"     "+(Integer)p.get(i));
//		}
//		ArrayList<String> t = new ArrayList();
//		t.add("id");
//		t.add("gpa");
//		t.add("name");
//		t.add("dob");
//
//		Hashtable<String, Object> row1 = new Hashtable();
//		row1.put("id", new Integer(4));
//		row1.put("name", new String("Zaky Noor"));
//		row1.put("gpa", new Double(0.88));
//		row1.put("dob", new Date(1995 - 1900, 4 - 1, 1));
//
//		Hashtable<String, Object> row2 = new Hashtable();
//		row2.put("id", new Integer(8));
//		row2.put("name", new String("Loshina"));
//		row2.put("gpa", new Double(0.7));
//		row2.put("dob", new Date(1997 - 1900, 10 - 1, 1));
//		
//		Hashtable<String, Object> row3 = new Hashtable();
//		row3.put("id", new Integer(18));
//		row3.put("name", new String("Gego"));
//		row3.put("gpa", new Double(0.9));
//		row3.put("dob", new Date(1997 - 1900, 10 - 1, 1));
//		
//		Hashtable<String, Object> row4 = new Hashtable();
//		row4.put("id", new Integer(5));
//		row4.put("name", new String("Dareen"));
//		row4.put("gpa", new Double(0.8));
//		row4.put("dob", new Date(2000 - 1900, 2 - 1, 9));
//		
//		Table ta = new Table("Loji");
//		System.out.println(ta);
//		
//		ta.insertInTable("Loji", row1);//0
////		ta.insertInTable("Loji", row1);//1
////	    ta.insertInTable("Loji", row1);//2
//
//		ta.insertInTable("Loji", row4);//3
//		ta.insertInTable("Loji", row2);//4
//		ta.insertInTable("Loji", row3);//5
//		// System.out.println(ta.page.size());
//		File f1 = new File("src/main/resources/data/pcs_0.pages");
//		f1.delete();
	    
	    	
//	    	ta.removeEmptyPages("pcs",ta.page,0);   

//		Date t1 = new Date(1997 - 1900, 10 - 1, 1);
//		Date t2 = new Date(2000 - 1900, 2 - 1, 12);
//		Date t3 = new Date( 2 , 2 - 1, 2000 - 1900);
//		
//		System.out.println("t1 " + t1.toString());
//		System.out.println( "isValidFormat (1997 - 1900, 10 - 1, 1 )" + isValidFormat(t1.toString()));
//		System.out.println( "isValidFormat (2000 - 1900, 2 - 1, 12 )" + isValidFormat(t2.toString()));
//		System.out.println( "isValidFormat (2 , 2 - 1, 2000 - 1900 )" + isValidFormat(t3.toString()));

//		
//		if(t1.compareTo(t2) ==0 ) {
//			System.out.println("DATES ARE EQUAL");
//		}
//		if(t1.compareTo(t2) > 0 ) {
//			System.out.println("t1 is greater than t2");
//			}
//		
//		if(t1.compareTo(t2) < 0 ) {
//			System.out.println("t1 is less than t2");
//			}
//		SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.US);
//		Date parsedDate = sdf.parse(t1);
//		Date parsedDate2 = sdf.parse(t2);
//		
//		SimpleDateFormat date1 = new SimpleDateFormat("yyyy-MM-DD");
//	
//		System.out.println("TUPLE DATE " +date1.format(parsedDate2));
//		System.out.println("INPUT DATE "+date1.format(parsedDate));
//		cmin = (date1.format(parsedDate2)).compareTo(date1.format(parsedDate));
//		
//		
		

		
//		
	//	ta.removeEmptyPages("pcs",ta.page,0);
		
//		Hashtable<String, Object> rowUpdate = new Hashtable();
//		rowUpdate.put("name", new String("araf"));
//
////		ArrayList td = new ArrayList();
////		td.add("name");
////		td.add("Dareen");
////		td.add("id");
////		td.add(5);
////		td.add("dob");
////		td.add(new Date(2000 - 1900, 2 - 1, 9));
////		
//		
//		updateT("Loji", "5", rowUpdate);
//		
		
//		
//		System.out.println(   ((pages)(ta.page.get(0))).tuples.size());
//		System.out.println(((pages) (ta.page.get(0))).tuples.get(3).clusterKey);
//		System.out.println(chkdeletion(td, ((pages) (ta.page.get(0))).tuples.get(3), "Loji"));
//		System.out.println(readCsvCKDataType("Loji"));
//		
//		Hashtable<String, Object> rowx = new Hashtable();
//		rowx.put("name", new String("Dareen"));
//		rowx.put("id", new Integer(20));
//		
		
//        System.out.println("SIZE pf page 0 BEFORE " + (((pages)(ta.page.get(0))).count));
//        System.out.println("SIZE of page 1 BEFORE " + (((pages)(ta.page.get(1))).count));
//		deleteFromT("Loji", rowx);
//        System.out.println("SIZE pf page 0 After " + (((pages)(ta.page.get(0))).count));
//        System.out.println("SIZE of page 1 BEFORE " + (((pages)(ta.page.get(1))).count));
		String fileName = "src/main/resources/data/tables.txt";
		Vector<Table>tt = new Vector<Table>();
		Table t=null;
        File file = new File(fileName);
        if (file.exists()) {
        	FileInputStream fileStream = new FileInputStream(fileName);
     	     ObjectInputStream   is = new ObjectInputStream(fileStream);
     	     tt = (Vector) is.readObject();
     	     for(int i =0;i<tt.size();i++) {
     	    	if((((Table)(tt.get(i))).name).equals("pcs")) {
     	    		t=((Table)(tt.get(i)));
     	    	   
     	    	}
     	     }  
       }
        System.out.println(t);
		Vector page = t.readTablePages("pcs");
		pages x= (pages)page.get(0);
		System.out.println(x.size());
		System.out.println(x.readOverflowVector("pcs"));
		t.removeEmptyPages("pcs", x.readOverflowVector("pcs"), 2);

	}
}
