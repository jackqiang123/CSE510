package tests;

import iterator.*;
import heap.*;
import global.*;
import index.*;

import java.io.*;
import java.util.*;
import java.lang.*;

import dbinterface.*;
import dbinterface.CreateCommand;
import dbinterface.Parser;
import diskmgr.*;
import bufmgr.*;
import btree.*;
import catalog.*;

// here we create shape table and insert such records

class ColaMarket {
	public int mid;
	public String mname;
	public Shape shape;

	public ColaMarket(int mid, String mname, Shape myshape) {
		this.mid = mid;
		this.mname = mname;
		this.shape = myshape;
	}
}

class MyJoinsDriver implements GlobalConst {

	private boolean OK = true;
	private boolean FAIL = false;
	private Vector colamrket;

	/**
	 * Constructor
	 */
	public MyJoinsDriver() {

		// build Sailor, Boats, Reserves table
		colamrket = new Vector();

		colamrket.addElement(new ColaMarket(53, "Bob Holloway", new Shape(1, 2,
				3, 4)));
		colamrket.addElement(new ColaMarket(54, "Susan Horowitz", new Shape(1,
				2, 3, 4)));
		colamrket.addElement(new ColaMarket(57, "Yannis Ioannidis", new Shape(
				1, 2, 3, 4)));

		boolean status = OK;
		int numscolamrket = 3;
		int numsailors_attrs = 3;

		String dbpath = "/tmp/" + System.getProperty("user.name")
				+ ".minibase.jointestdb";
		String logpath = "/tmp/" + System.getProperty("user.name") + ".joinlog";

		String remove_cmd = "/bin/rm -rf ";
		String remove_logcmd = remove_cmd + logpath;
		String remove_dbcmd = remove_cmd + dbpath;
		String remove_joincmd = remove_cmd + dbpath;

		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
			Runtime.getRuntime().exec(remove_joincmd);
		} catch (IOException e) {
			System.err.println("" + e);
		}

		/*
		 * ExtendedSystemDefs extSysDef = new ExtendedSystemDefs(
		 * "/tmp/minibase.jointestdb", "/tmp/joinlog", 1000,500,200,"Clock");
		 */

		SystemDefs sysdef = new SystemDefs(dbpath, 1000, NUMBUF, "Clock");

		// creating the sailors relation
		AttrType[] Mtypes = new AttrType[3];
		Mtypes[0] = new AttrType(AttrType.attrInteger);
		Mtypes[1] = new AttrType(AttrType.attrString);
		Mtypes[2] = new AttrType(AttrType.attrShape);

		// Size of the market
		short[] Msizes = new short[1];
		Msizes[0] = 30; // first elt. is 30

		Tuple t = new Tuple();
		try {
			t.setHdr((short) 3, Mtypes, Msizes);
		} catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			status = FAIL;
			e.printStackTrace();
		}

		int size = t.size();

		RID rid;
		Heapfile f = null;
		try {
			f = new Heapfile("colamarket.in");
		} catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			status = FAIL;
			e.printStackTrace();
		}

		t = new Tuple(size);
		System.out.println("sdfsafsa" + t.noOfFlds());
		try {
			t.setHdr((short) 3, Mtypes, Msizes);
		} catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			status = FAIL;
			e.printStackTrace();
		}

		for (int i = 0; i < numscolamrket; i++) {
			try {
				t.setIntFld(1, ((ColaMarket) colamrket.elementAt(i)).mid);
				t.setStrFld(2, ((ColaMarket) colamrket.elementAt(i)).mname);
				t.setShapeFld(3, ((ColaMarket) colamrket.elementAt(i)).shape);
		
			} catch (Exception e) {
				System.err
						.println("*** Heapfile error in Tuple.setStrFld() ***");
				status = FAIL;
				e.printStackTrace();
			}

			try {
				rid = f.insertRecord(t.returnTupleByteArray());
			} catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				status = FAIL;
				e.printStackTrace();
			}
			System.out.println(t.returnTupleByteArray().length + "--" + t.getLength()); 
		}
		if (status != OK) {
			// bail out
			System.err.println("*** Error creating relation for sailors");
			Runtime.getRuntime().exit(1);
		}

	}

	public boolean runTests() {

		// Disclaimer();
		Query1();

		// Query2();
		// Query3();

		// Query4();
		// Query5();
		// Query6();

		System.out.print("Finished joins testing" + "\n");

		return true;
	}

	public void Query1() {
		// creating the sailors relation
		String test1 = "CREATE TABLE cola_markets (mkt_id NUMBER PRIMARY KEY, name VARCHAR2(32), shape SDO_GEOMETRY)";
		Command cmd = Parser.createCommand(test1);
	//	CreateCommand aa = new CreateCommand(cmd);
		AttrType[] Mtypes = new AttrType[3];
		Mtypes[0] = new AttrType(AttrType.attrInteger);
		Mtypes[1] = new AttrType(AttrType.attrString);
		Mtypes[2] = new AttrType(AttrType.attrShape);
		   FldSpec [] Sprojection = new FldSpec[3];
		    Sprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		    Sprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		   Sprojection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);

		// Size of the market
		short[] Msizes = new short[1];
		Msizes[0] = 30; // first elt. is 30

		 FileScan am = null;
		    try {
		      am  = new FileScan("colamarket.in", Mtypes, Msizes, 
						  (short)3, (short)3,
						  Sprojection, null);
		    }
		    catch (Exception e) {
			      System.err.println (""+e);
		    }
		
		    System.out.println("i am checking reading");
		  			try {
		  			Tuple t1 = am.get_next();

		  		//	if (t1 == null) System.out.println("empty tul");
				System.out.println(t1.getIntFld(1)+ " ---- " + t1.getStrFld(2));
			Shape a = t1.getShapeFld(3);
			System.out.println(a.x1+" "+a.x2+" "+a.y1+" "+a.y2);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 		    
		    System.out.println("Query 1 is finished!");
		
	}

}

public class ShapeTest {
	public static void main(String argv[]) {
		boolean sortstatus;
		// SystemDefs global = new SystemDefs("bingjiedb", 100, 70, null);
		// JavabaseDB.openDB("/tmp/nwangdb", 5000);

		MyJoinsDriver jjoin = new MyJoinsDriver();

		sortstatus = jjoin.runTests();
		if (sortstatus != true) {
			System.out.println("Error ocurred during join tests");
		} else {
			System.out.println("join tests completed successfully");
		}
	}
}