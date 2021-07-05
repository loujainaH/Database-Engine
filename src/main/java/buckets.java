import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.Vector;


public class buckets implements Serializable{
  Vector<pair>bucket; 
    Vector overflowBucket ;

	
    public buckets() {
    Vector<pair>bucket= new Vector<pair>(); 
    Vector overflowBucket= new Vector();
    }
    //el bucket name bey5osh gahez
    public void insertinbucket(String pageName, int rowN , String bucketName ) throws ClassNotFoundException, IOException {

    	int[]config =DBApp.getconfig();
    	int n = config[1];
   // 	System.out.println("ana el n el gamilaa---"+n);
    	
    	this.bucket=readBucket(bucketName);
    	this.overflowBucket=readOverflowBucket(bucketName);
    	String obn=bucketName;
    	if (this.bucket.size()==n) {// general overflow 
    		if (overflowBucket.size()==0) {
	    		System.out.println("first overflow bucket");
	    		overflowBucket o = new overflowBucket();
	    		bucketName+="_overflow_0";
	//    		o.insertinbucket(pageName, rowN,bucketName);
	    		o.bucket=new Vector<pair>();
	    		o.bucket.add(new pair(pageName,rowN));
	    		overflowBucket.add(o);
	    		writeToBucket(bucketName,o.bucket);
    		}
    		else  {
    			int i = overflowBucket.size()-1;
                Vector<pair>p =readBucket(bucketName+"_overflow_"+i);
                if (p.size()==n) {
                	overflowBucket o = new overflowBucket();
                	bucketName+="_overflow_"+overflowBucket.size();
                	o.bucket=new Vector<pair>();
                	o.bucket.add(new pair(pageName,rowN));
//                	o.insertinbucket(pageName, rowN,bucketName);
                	overflowBucket.add(o);
                	writeToBucket(bucketName,o.bucket);
                }
                else {
                	bucketName+="_overflow_"+i;
                	p.add(new pair(pageName,rowN));
                	writeToBucket(bucketName,p);
//                	(((overflowBucket)(overflowBucket.get(i))).bucket)
                }
    			
    		}
    		
    		}
    	else {
    		
    		bucket.add(new pair(pageName,rowN));
    		if(bucket.size() != 0) {
    			writeToBucket(bucketName,this.bucket);
    	}}
    	if(this.overflowBucket.size() != 0 ) {
    		writeOverflowBucket(obn,this.overflowBucket);
    }
    	Collections.sort(bucket);	
    	
    }
    
	public  void writeToBucket(String bucketName , Vector<pair>bucket) throws FileNotFoundException, IOException {
		String fileName = "src/main/resources/data/" + bucketName+ ".buckets";
	    try {
	    	ObjectOutputStream os =new ObjectOutputStream(new FileOutputStream(fileName) );
	    	os.writeObject(bucket);
	    	os.close();	
		} catch (Exception e) {
			// TODO: handle exception
		}    
  }
	
	public  Vector<pair> readBucket (String bucketName) throws IOException, ClassNotFoundException {
		String fileName = "src/main/resources/data/" + bucketName+ ".buckets";
		 	Vector<pair>tt = new Vector<pair>();
	        File file = new File(fileName);
	        if (file.exists()) {
	        	FileInputStream fileStream = new FileInputStream(fileName);
	     	     ObjectInputStream   is = new ObjectInputStream(fileStream);
	     	     tt = (Vector<pair>) is.readObject();
	            return tt;
	       }
        return tt; 
	}
	
	public  void writeOverflowBucket(String tableName, Vector  t) throws FileNotFoundException, IOException {
		String fileName = "src/main/resources/data/" + tableName+"_overflow.buckets";
	    try {
	    	ObjectOutputStream os =new ObjectOutputStream(new FileOutputStream(fileName) );
	    	os.writeObject(t);
	    	os.close();	
		} catch (Exception e) {
			// TODO: handle exception
		}    
  }
	
	public  Vector readOverflowBucket (String tableName) throws IOException, ClassNotFoundException {
		String fileName = "src/main/resources/data/" + tableName+"_overflow.buckets";
		Vector tt = new Vector<overflowBucket>();
        File file = new File(fileName);
        if (file.exists()) {
        	 FileInputStream fileStream = new FileInputStream(fileName);
     	     ObjectInputStream   is = new ObjectInputStream(fileStream);
     	     tt = (Vector) is.readObject();
     	     return tt ;
     	     }  
        return tt; 
}
	
	public static void main(String[] args) throws ClassNotFoundException, IOException {
		buckets b = new buckets();
		for (int i =0;i<203;i++) {
			b.insertinbucket("gego", 2, "home_g0_101");
		}
		
		Vector<pair> gego = b.readBucket("home_g0_101");
		System.out.println(gego.get(0).max);
      System.out.println("NUMBER OF RECORDS IN PAGE 0 : "+(b.readBucket("home_g0_101")).size());
      System.out.println("NUMBER OF RECORDS IN PAGE 0 OVERFLOW NUMBER 0: "+((b.readBucket("home_g0_101_overflow_0")).size()));
      System.out.println("NUMBER OF RECORDS IN PAGE 0 OVERFLOW NUMBER 1: "+((b.readBucket("home_g0_101_overflow_1")).size()));
      System.out.println("NUMBER OF RECORDS IN PAGE 0 OVERFLOW NUMBER 2: "+((b.readBucket("home_g0_101_overflow_2")).size()));
	}
	
    
}
