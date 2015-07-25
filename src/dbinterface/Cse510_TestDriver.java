package dbinterface;

//import global.SystemDefs;

import java.io.*; 

import global.*;
import bufmgr.*;
import diskmgr.*;
import heap.*;
import iterator.*;
import index.*;
import btree.*; 

import java.util.List;
import java.util.Random;

import tests.TestDriver;

/* Co-author: Xiaoyu Zhang
 *		Bing Han        
 *		Brian Vincent
 */

public class Cse510_TestDriver
{
		
  public static void main(String argv[])
  {
    boolean indexstatus;

    CSE510Tests tests = new CSE510Tests();

    indexstatus = tests.runTests();
    
    
    if (indexstatus != true) {
      System.out.println("Error ocurred during index tests");
    }
    else {
      System.out.println("Index tests completed successfully");
    }
  }
}


class CSE510Tests extends TestDriver implements GlobalConst
{
	
	private Command cmd;
	private String test;
	
	public CSE510Tests()
	{
		super("cse510Tests");
	}
	
	
	public boolean runTests ()  {
	    
	    System.out.println ("\n" + "Running " + testName() + " tests...." + "\n");
	    
	    SystemDefs sysdef = new SystemDefs( dbpath, 300, NUMBUF, "Clock" );

	    // Kill anything that might be hanging around
	    String newdbpath;
	    String newlogpath;
	    String remove_logcmd;
	    String remove_dbcmd;
	    String remove_cmd = "/bin/rm -rf ";

	    newdbpath = dbpath;
	    newlogpath = logpath;

	    remove_logcmd = remove_cmd + logpath;
	    remove_dbcmd = remove_cmd + dbpath;

	    // Commands here is very machine dependent.  We assume
	    // user are on UNIX system here
	    try {
	      Runtime.getRuntime().exec(remove_logcmd);
	      Runtime.getRuntime().exec(remove_dbcmd);
	    } 
	    catch (IOException e) {
	      System.err.println (""+e);
	    }
	    
	    remove_logcmd = remove_cmd + newlogpath;
	    remove_dbcmd = remove_cmd + newdbpath;

	    //This step seems redundant for me.  But it's in the original
	    //C++ code.  So I am keeping it as of now, just in case I
	    //I missed something
	    try {
	      Runtime.getRuntime().exec(remove_logcmd);
	      Runtime.getRuntime().exec(remove_dbcmd);
	    } 
	    catch (IOException e) {
	      System.err.println (""+e);
	    }

	    //Run the tests. Return type different from C++
	    boolean _pass = runAllTests();

	    
	    
	    
	    

	    List<data> l = CreateTableFunction.tableinfo.get("cola_markets");
		
		
		//read attribute list
		AttrType [] mtype = new AttrType[l.size()];
		int fldNum = l.size();
		int StringCount = 0; // number of string filed
		for (int i = 0; i < fldNum;i++)
		{
			if (l.get(i).type.equals("NUMBER"))
				mtype[i] = new AttrType(AttrType.attrInteger);
			else if (l.get(i).type.startsWith("VARCHAR"))
			{StringCount++;	mtype[i] = new AttrType(AttrType.attrString);}
			else if (l.get(i).type.startsWith("SDO"))
				mtype[i] = new AttrType(AttrType.attrShape);
			else 
				mtype[i] = new AttrType(AttrType.attrReal);
			
		}
		
		
		//Msizes length is number of string attributes
		short [] Msizes = new short [StringCount];
	
		for (int i = 0; i<Msizes.length;i++)
		{
			Msizes[i] = 10;
		}
		

	    
		FldSpec [] Sprojection = new FldSpec[l.size()];
		for(int i=0;i<fldNum;i++)
		{
	    Sprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
		}
		
		//fieldsnum are equal because there is no projections
		 FileScan am = null;
		    try 
		    {
		      am  = new FileScan("cola_markets", mtype, Msizes, 
						  (short) fldNum, (short) fldNum,
						  Sprojection, null);
		    }
		    catch (Exception e) 
		    {
			      System.err.println (""+e);
		    }
		
		    System.out.println("i am checking reading");
		  	try 
		  	{
		  		while(true)
		  		{
		  			Tuple t1 = am.get_next();
		  			if (t1 == null) break;
		  			//	if (t1 == null) System.out.println("empty tul");
		  			System.out.println(t1.getIntFld(1)+ " ---- " + t1.getStrFld(2));
		  			Shape a = t1.getShapeFld(3);
		  			System.out.println(a.x1+" "+a.x2+" "+a.y1+" "+a.y2);
		  		}
		  	} catch (Exception e) 
		  	{
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 		    

		  			
	    

		  	
		  	//Index scan time!
		    FldSpec[] projlist = new FldSpec[2];
		    RelSpec rel = new RelSpec(RelSpec.outer); 
		    projlist[0] = new FldSpec(rel, 1);
		    projlist[1] = new FldSpec(rel, 2);
		    
		    
		    // start index scan
		    IndexScan iscan = null;
		    try {
		      iscan = new IndexScan(new IndexType(IndexType.B_Index), "cola_markets", "cola_spatial_idx", mtype, Msizes, 3, 3, Sprojection, null, 3, true);
		    }
		    catch (Exception e) {
		    	System.out.println(e.getMessage());
				e.printStackTrace();
		    }
		    

		    int count = 0;
		    Tuple t = null;
		    String outval = null;
		    
		    
		    try {
		      t = iscan.get_next();
		    }
		    catch (Exception e) {
		    	System.out.println(e.getMessage());
				e.printStackTrace();
		    }
		    System.out.println();
		    System.out.println("INDEX SCAN (sorted):");
		    while(t!= null)
		    {
		    	
		    	try 
		    	{
		    		System.out.println("\tx1: " + t.getTupleByteArray()[9] + " x2: " + t.getTupleByteArray()[13] + " y1: " + t.getTupleByteArray()[17] + " y2: " + t.getTupleByteArray()[21]);
				    t = iscan.get_next();
				}
		    	catch (Exception e) 
		    	{
		    		
				 	System.out.println(e.getMessage());
					e.printStackTrace();
				}
		    	
		    }
	    
	    
	    
	    //Clean up again
	    try {
	      Runtime.getRuntime().exec(remove_logcmd);
	      Runtime.getRuntime().exec(remove_dbcmd);
	    } 
	    catch (IOException e) {
	      System.err.println (""+e);
	    }
	    
	    System.out.println ("\n" + "..." + testName() + " tests ");
	    System.out.println (_pass==OK ? "completely successfully" : "failed");
	    System.out.println (".\n\n");
	    
	    return _pass;
	  }
	
	
	protected boolean test1()
	{
		test = "CREATE TABLE cola_markets (mkt_id NUMBER PRIMARY KEY, name VARCHAR(32), shape SDO_GEOMETRY)";
		
		cmd = Parser.createCommand(test);
		cmd.Start();	//creates Heapfile
				
		return true;
	}
	
	protected boolean test2()
	{
		

		test = "INSERT INTO cola_markets VALUES(1, 'cola_a', SHAPE(1, 2, 4, 4));";

		cmd = Parser.createCommand(test);
		cmd.Start();
		

		test = "INSERT INTO cola_markets VALUES(1, 'cola_b', SHAPE(6, 9, 1, 0));";

		cmd = Parser.createCommand(test);
		cmd.Start();
		

		test = "INSERT INTO cola_markets VALUES(3, 'cola_c', SHAPE(3, 3, 7, 6));";
		
		cmd = Parser.createCommand(test);
		cmd.Start();
		
		
		test = "INSERT INTO cola_markets VALUES(3, 'cola_c', SHAPE(3, 4, 5, 6));";
		
		cmd = Parser.createCommand(test);
		cmd.Start();
		
		
		test = "INSERT INTO cola_markets VALUES(4, 'cola_d', SHAPE(4, 1, 7, 2));";
		
		cmd = Parser.createCommand(test);
		cmd.Start();


				
				
		        List<data> l = CreateTableFunction.tableinfo.get("cola_markets");
				
				
				//read attribute list
				AttrType [] mtype = new AttrType[l.size()];
				int fldNum = l.size();
				int StringCount = 0; // number of string filed
				for (int i = 0; i < fldNum;i++)
				{
					if (l.get(i).type.equals("NUMBER"))
						mtype[i] = new AttrType(AttrType.attrInteger);
					else if (l.get(i).type.startsWith("VARCHAR"))
					{StringCount++;	mtype[i] = new AttrType(AttrType.attrString);}
					else if (l.get(i).type.startsWith("SDO"))
						mtype[i] = new AttrType(AttrType.attrShape);
					else 
						mtype[i] = new AttrType(AttrType.attrReal);
					
				}
				
				
				//Msizes length is number of string attributes
				short [] Msizes = new short [StringCount];
			
				for (int i = 0; i<Msizes.length;i++)
				{
					Msizes[i] = 10;
				}
				

		


			    
	FldSpec [] Sprojection = new FldSpec[l.size()];
	for(int i=0;i<fldNum;i++)
	{
    Sprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
	}
	
	//fieldsnum are equal because there is no projections
	 FileScan am = null;
	    try {
	      am  = new FileScan("cola_markets", mtype, Msizes, 
					  (short) fldNum, (short) fldNum,
					  Sprojection, null);
	    }
	    catch (Exception e) {
		      System.err.println (""+e);
	    }
	
	    System.out.println("data record inserted");
	  			try {
	  				while(true){
	  			Tuple t1 = am.get_next();
	  			if (t1 == null) break;
	  		//	if (t1 == null) System.out.println("empty tul");
		System.out.print(t1.getIntFld(1)+ " ---- " + t1.getStrFld(2));
		Shape a = t1.getShapeFld(3);
		System.out.println("[" + a.x1+" "+a.x2+" "+a.y1+" "+a.y2 +"]");
		}} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 		   
				
		
		return true;
	}
	
	protected boolean test3()
	{
		test = "CREATE INDEX cola_spatial_idx ON cola_markets(shape)";
		
		cmd = Parser.createCommand(test);
		cmd.Start();
		
		
		return true;
	}
	
	
		protected boolean test4()
	{
			String test4 = "SELECT SDO_GEOM.SDO_INTERSECTION(c_a.shape, c_c.shape, 0.005) FROM cola_markets c_a, cola_markets c_c WHERE c_a.name = 'cola_a' AND c_c.name = 'cola_c'";		cmd = Parser.createCommand(test4);
			cmd = Parser.createCommand(test4);

			cmd.Start();
	
		return true;
	}
	
	protected boolean test5()
	{
		String test4 = "SELECT c.name, SDO_GEOM.SDO_AREA(c.shape, 0.005) FROM cola_markets c WHERE c.name = 'cola_a'";
		cmd = Parser.createCommand(test4);
		cmd.Start();
	

	
		return true;
	}
	
	protected boolean test6()
	{
	
		String test6 = "SELECT SDO_GEOM.RELATE(c_b.shape, 'anyinteract', c_d.shape, 0.005) FROM cola_markets c_b, cola_markets c_d WHERE c_b.name = 'cola_a' AND c_d.name = 'cola_c'";
		
		cmd = Parser.createCommand(test6);
		cmd.Start();
	

		return true;
	}
	
	
	
	
}
