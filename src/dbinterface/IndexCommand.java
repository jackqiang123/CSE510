package dbinterface;
import global.AttrType;
import global.RID;
import global.Shape;
import heap.*;
import iterator.*;

import java.io.IOException;
//Author: Xiaoyu Zhang
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
// Author : Bing
// Author : Brian Vincent
// CREATE INDEX cola_spatial_idx
//  ON cola_markets(shape)
//  INDEXTYPE IS MDSYS.SPATIAL_INDEX;

import btree.BTreeFile;
import btree.IntegerKey;
import btree.KeyClass;
import btree.ShapeKey;
import btree.StringKey;

public class IndexCommand extends Command
{
	//constructor
   private String indexname;
   private String tablename;
   private String columename;
   private String indextype;
   private static short REC_LEN2 = 160;
   
   public IndexCommand(String command) {
	   this._cmd = command;
	   String[] attr = new String[0];
	   String[] com = command.split(" ");
	   String test = com[4];
	   //String[] attr = com[7].split(";");
	   int breakpoint = 0;
	   String tblName = null;
	   for(int i=0;i<test.length();i++)
	   {
		   if(test.charAt(i) == '(')
		   {
			   tblName = test.substring(breakpoint,i);
			   breakpoint = i+1;
		   }
		   //System.out.println(test2);
	   }
	   String colName = test.substring(breakpoint, test.length()-1);

	   if(com.length <= 5)
	   {   
	   if((com[0]+com[1]).equals("CREATEINDEX") == false || com[3].equals("ON") == false || com.length < 5)
       {
		   System.out.println("wrong branches!");
	   }
	   else {
	  indexname = com[2];
	  columename = colName;
	  tablename = tblName;
	  indextype = null; 
	   }
	   }		   
	   
//	   else if(com.length <= 8)
//	   {   
//		   if(com.length < 8 || ((com[5]+com[6]).equals("INDEXTYPEIS") == false))
//		   {
//		   System.out.println("wrong branches");
//		   }
//		   else
//		   {
//			 indexname = com[2];
//			 tablename = tblName;
//			 columename = colName;
//			 indextype = attr[0]; 
//		   }
//	   }
	   
   }
		
	//@override
	public void Start() 
	{
//		System.out.println();		
//		System.out.println(this._cmd);
//		System.out.println("\tCreate Index: " + indexname);
//		System.out.println("\tTable: " + tablename);
//		System.out.println("\tColumn Name: " + columename);
//		System.out.println("\tKeytype: " + indextype);
		
		Heapfile f = null;
		int keyColumn = 0;
		
		//attempt to recall heapfile
		try
		{
			f = new Heapfile(this.tablename); 
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		
		
		//get attribute type
		AttrType[] indxType = new AttrType[1];
		List<data> l = CreateTableFunction.tableinfo.get(this.tablename);
		for (int ii = 0; ii < l.size(); ii++)
		{
			//String s = l.get(ii).name;
			if (l.get(ii).name.equals(this.columename))
			{
				keyColumn = ii + 1;
				String tmpString = l.get(ii).type;
				if (tmpString.equals("NUMBER"))
					indxType[0] = new AttrType(AttrType.attrInteger);
				else if (tmpString.startsWith("VARCHAR"))
					indxType[0] = new AttrType(AttrType.attrString);
				else if (tmpString.startsWith("SDO"))
					indxType[0] = new AttrType(AttrType.attrShape);
				else 
					indxType[0] = new AttrType(AttrType.attrReal);
			}
		}
		
		
		
		//create index
		BTreeFile btf = null;
	    try {
	      btf = new BTreeFile(this.indexname, indxType[0].attrType, 16, 1/*delete*/); 
	    }
	    catch (Exception e) {
	      
	    	System.out.println(e.getMessage());
			e.printStackTrace();
	    }
		
	    
	    //create representative tuple
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
		
		short [] Msizes = new short [StringCount];
	
		for (int i = 0; i<Msizes.length;i++)
		{
			Msizes[i] = 10;
		}
		
		boolean status = true;
		Tuple t = new Tuple();
		try {
			t.setHdr((short) fldNum, mtype, Msizes);
		} catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
		    status = false;
			e.printStackTrace();
		}

		int size = t.size();

		t = new Tuple(size);
		try 
		{
				t.setHdr((short) fldNum, mtype, Msizes);
		} 
		catch (Exception e) 
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	    
	    
	    
//		//populate index file and create an scan on the heapfile
	    Scan scan = null;
	    RID rid = new RID();
	    Tuple temp = null;
//	    
//	    Tuple t = new Tuple();
//	    
	    //returning a tuple from scan
	    try 
	    {
	    	scan = new Scan(f);
	    	temp = scan.getNext(rid);
	    }
	    catch (Exception e) 
	    {
	    	System.out.println(e.getMessage());
			e.printStackTrace();
	    }
//	    
//	    //iterating through heapfile and extracting all tuples
	    while ( temp != null) 
	    {
	    	t.tupleCopy(temp);
		    
	    	//extracting key from returned tuple
		    try 
		    {
		    	switch(indxType[0].attrType)
		    	{
		    	case AttrType.attrInteger:
		    		int iKey = t.getIntFld(keyColumn);
		    		btf.insert(new IntegerKey(iKey), rid);
		    		break;
		    	case AttrType.attrString:
		    		String stKey = t.getStrFld(keyColumn);
		    		btf.insert(new StringKey(stKey), rid);
		    		break;
		    	case AttrType.attrShape:
		    		Shape shKey = t.getShapeFld(keyColumn);
		    		btf.insert(new ShapeKey(shKey), rid);
		    		break;
		    	}
		    }
		    catch (Exception e) 
		    {
			    System.out.println(e.getMessage());
				e.printStackTrace();
		    }
		      

	
		    try 
		    {
		    	temp = scan.getNext(rid);
		    }
		    catch (Exception e) 
		    {
		    	System.out.println(e.getMessage());
				e.printStackTrace();
		    }
	    }
	    
	    // close the file scan
	    scan.closescan();
	    System.out.println("BTreeIndex file created successfully.\n"); 
		
		
		
		
		
		
	}
}
