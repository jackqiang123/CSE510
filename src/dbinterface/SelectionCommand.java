package dbinterface;

/* Co-author: Lian Lu
 * 			  Bing Han
 *            Xiaoyu Zhang
 */

//SELECT SDO_GEOM.SDO_INTERSECTION(c_a.shape, c_c.shape, 0.005) 
//FROM cola_markets c_a, cola_markets c_c 
//WHERE c_a.name = 'cola_a' AND c_c.name = 'cola_c'; 
//
//
//SELECT c.name, SDO_GEOM.SDO_AREA(c.shape, 0.005) 
//FROM cola_markets c 
//WHERE c.name = 'cola_a'; 
//
//SELECT SDO_GEOM.RELATE(c_b.shape, 'anyinteract', c_d.shape, 0.005) 
//FROM cola_markets c_b, cola_markets c_d 
//WHERE c_b.name = 'cola_b' AND c_d.name = 'cola_d'; 

import global.AttrType;
import global.Shape;
import heap.FieldNumberOutOfBoundException;
import heap.Tuple;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;

import java.awt.Rectangle;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class SelectionCommand extends Command
{
	private List<table> fromtable;
	private List<condition> wheretable;
	private String SelectSpatial = "";
	private List<String> spatialaugment;
	private List<String> augment;
		
	public SelectionCommand(String cmd)
	{
		this._cmd = cmd;
		//cmd = cmd.toUpperCase();
		String [] myset = cmd.split("SELECT|FROM|WHERE");
		// the com is lies in 1,2,3
		// we are now dealing with selection
		String select = myset[1].substring(1);
		//for(int i=0;i<myset.length;i++)
		//	System.out.println(myset[i]);
		//System.out.println(select);
		
		SelectSpatial = "";
		if (select.contains("SDO_GEOM.SDO_INTERSECTION"))
			SelectSpatial = "INTERSECTION";
		else if (select.contains("SDO_GEOM.SDO_AREA"))
			SelectSpatial = "AREA";
		else if (select.contains("SDO_GEOM.RELATE"))
			SelectSpatial = "RELATE";
		
		
		// SDO_GEOM.SDO_INTERSECTION(c_a.shape, c_c.shape, 0.005) 
		// SDO_GEOM.SDO_AREA(c.shape, 0.005) 
		// SDO_GEOM.RELATE(c_b.shape, 'anyinteract', c_d.shape, 0.005)
		spatialaugment = new ArrayList<String>();
		String wanted_string = "";
		int _breakpoint = 0;
		int endpoint = select.length()-1;
		for(int j=0;j<select.length();j++)
		{
			if(select.charAt(j) == '(')
			{
				_breakpoint = j+1;
				continue;
			}
			if(select.charAt(j) == ')')
			{
				endpoint = j;
				if(_breakpoint < endpoint)
				{
					wanted_string= select.substring(_breakpoint, endpoint).trim();
				}
			}
		}
		
		//System.out.println(wanted_string);
		String[] parts = wanted_string.split(", ");
		for(int i=0;i<parts.length;i++)
		{
			spatialaugment.add(new String(parts[i].trim()));
		}
		
		
		//
		augment = new ArrayList<String>();
		if (SelectSpatial.equals("") == false)
		{
			int left = 0 ;
				while (select.substring(left, left + 3).equals("SDO") == false)
				left ++ ;
			int right = left;
			while(select.charAt(right) != ')') right++;
			//we ignore from left to right
			
			
			String now = select.substring(0, left) + select.substring(right+1);
			String [] p = now.split(",");
			for (String s : p)
			{
				if (s.trim().equals(" ") == false && s.trim().equals("") == false)
				augment.add(s.trim());
			}
		}
		else {
			String[] p = select.split(",");
			for (String s : p)
			{
				if (s.trim().equals(" ") == false)
				augment.add(s.trim());
			}
		}
		
		//
		fromtable = new LinkedList<table>();
		String []from = myset[2].split(",");
		for (String f : from)
		{
			String p = f.trim();
			String [] temp = p.split(" ");
			fromtable.add(new table(temp[0],temp[1]));
		}
		
		wheretable = new LinkedList<condition>();
		String []where = myset[3].split(" AND ");
		for (String w : where)
		{
			String [] p = w.split("=");
			wheretable.add(new condition(p[0].trim(),p[1].trim()));
		}
	}
	
	public void Start()
	{
		
		//Heapfile f = null;
	

//		System.out.println();
//		System.out.println(this._cmd);
//		//System.out.println("We now show the Projection (Select Statement) augments: ");
//		if(augment.isEmpty())
//		{
//			System.out.print("null");
//		}
//		else
//		{
//			for (String p : augment)
//				System.out.print(p + ", ");
//		}
//		System.out.println();
//		if (this.SelectSpatial.equals("") == false)
//		{
//			System.out.print("\tSpatial Selection: " + SelectSpatial + " with the following augments: ");
//			for(int k=0; k<spatialaugment.size(); k++)
//			{
//				if(k!=spatialaugment.size()-1)
//					System.out.print(spatialaugment.get(k) + ", ");
//				else
//					System.out.print(spatialaugment.get(k));
//			}
//			
//		}
//		
//		System.out.println();
//		System.out.print("\tFROM: ");
//		for (table t : fromtable)
//		{
//			System.out.print(t.tablename + " whose alias is " + t.tablealias + "\t");
//		}
//		System.out.println();
//		System.out.print("\tWHERE: ");
//		for (int c=0; c<wheretable.size(); c++)
//		{
//			if(c!=wheretable.size()-1)
//				System.out.print(wheretable.get(c).getLft() + " = " + wheretable.get(c).getRgt() + " AND ");
//			else
//				System.out.println(wheretable.get(c).getLft() + " = " + wheretable.get(c).getRgt());
//		}
//		
		
		
		///
		String []tableName = new String[fromtable.size()];
		for (int i = 0; i< tableName.length;i++)
			tableName[i] = fromtable.get(i).tablename;
		
	    String []tablealis =  new String[fromtable.size()];
		for (int i = 0; i< tableName.length;i++)
			tablealis[i] = fromtable.get(i).tablealias;
		
		
		
		
		// List of # table - list
		List<List<Tuple>> tup = new LinkedList<List<Tuple>>(); 
		
		
		for (int ll = 0; ll <tableName.length;ll++)
		{
		
			List<data> l = CreateTableFunction.tableinfo.get(tableName[ll]);
			List<Tuple> tuples = new LinkedList<Tuple>();
			
			//read attribute list
			if(l != null)
			{
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
				   am  = new FileScan(tableName[ll], mtype, Msizes, 
								  (short) fldNum, (short) fldNum,
								  Sprojection, null);
				 }
				 catch (Exception e) {
					   System.err.println (""+e);
				 }
				 
				 try
				 {
					 while(true)
					 {
						 Tuple t = am.get_next();
						 if(t == null) 
							 break;
						 Tuple temp = new Tuple(t);
						 
						tuples.add(temp);
						 
						//System.out.println(t.getStrFld(2));
					 }
				 }
				 catch(Exception ex)
				 {
					 ex.printStackTrace();
				 } 
		
				 tup.add(tuples);
			}
		}
		// End of List of list
		
		
		//retrieve records
		int numofcondiction = wheretable.size();
			 
		List<List<Tuple>> tupafterrefine = new LinkedList<List<Tuple>>(); // satisfy from,where
		//System.out.println(" num of f " + numofcondiction);
		for (int i = 0; i < numofcondiction; i ++)
		{
			List<Tuple> l = new LinkedList<Tuple>();
			String left = wheretable.get(i).getLft();
			String right = wheretable.get(i).getRgt();
		//	System.out.println(right);
			
			String []tt = left.split("\\.");
     		if(tt.length>1)
			{
				String att  = tt[1];
		//		System.out.println(att);
				int index = 0;
			//	System.out.println(tt[0]);
				for (index = 0; index<tableName.length;index++)
				{//	System.out.println(index);
			//	System.out.println(tablealis[0]);
					if (tablealis[index].equals(tt[0].trim()))
					{			
						//System.out.println(tablealis[index]);
	
						break;
					}
				}
			//	System.out.println(index);
				List<data> tableinfor = CreateTableFunction.tableinfo.get(tableName[index]);
				List<Tuple> ll = new LinkedList<Tuple>();
				ll = tup.get(index);
				int nooffield = 1;
				for (;nooffield<=tableinfor.size();nooffield++)
				{
					if (tableinfor.get(nooffield-1).name.equals(att))
						break;
				}
				for (Tuple la : ll)
				{
					try
					{
						if ((la.getStrFld(nooffield)).equals(right))
						{ l.add(la);//System.out.println("we add a result with index " + index);
						}
					} catch (FieldNumberOutOfBoundException | IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
					}
				}
			//	System.out.println("current idnex is " + index);
							tupafterrefine.add(index, l);
			}
		}
		//End retriving record
	//	System.out.println("size " + tupafterrefine.size());	 
//List<Tuple> lll = tupafterrefine.get(0);
//try {
//	System.out.println(lll.get(0).getStrFld(2));
//} catch (FieldNumberOutOfBoundException | IOException e1) {
//	// TODO Auto-generated catch block
//	e1.printStackTrace();
//}
//		
		
			 if(SelectSpatial.equals("AREA"))
			 {
				 //test4 start
				 String str = augment.get(0);
				// System.out.println(str);
				 String[] ss = str.split("\\.");
				 String candi_alis = "NIL";
				 if(ss.length > 0) 
					 candi_alis = ss[0];
				 
				 int ind;
				 for(ind = 0; ind<tableName.length;ind++)
				 {
						if (tablealis[ind].equals(candi_alis))
						{
							break;
						}
				 }
				 
			//	 System.out.println(ind);
				// System.out.println(ss[1]);

				 List<Tuple> _l = tupafterrefine.get(ind);
//				 System.out.println(_l.size());
//				 for (Tuple a : _l)
//				 {
//					 try {
//						System.out.println(a.getStrFld(2));
//					} catch (FieldNumberOutOfBoundException | IOException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//				 }
				 List<data> tableinfo = CreateTableFunction.tableinfo.get(tableName[ind]);

				 int attno = 1;
				 
				 for (;attno<=tableinfo.size();attno++)
				 {
					// System.out.println(tableinfo.get(attno-1).type);
					 if (tableinfo.get(attno-1).type.startsWith("VARCHAR"))
						 break;
				 }
				// System.out.println("name index " + attno);
				 int shapedim = 1;
				 for (;shapedim<=tableinfo.size();shapedim++)
				 {
					 if (tableinfo.get(shapedim-1).type.startsWith("SDO"))
						 break;
				 }
				 
				 for (Tuple k : _l)
				 {
					 Shape sh = null;
					 String name = "";
					 try {
						 name = k.getStrFld(attno);
						
					} catch (FieldNumberOutOfBoundException | IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					 try {
						 sh = k.getShapeFld(shapedim);
					} catch (FieldNumberOutOfBoundException | IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					 int area = (sh.y1-sh.y2) * (sh.x1-sh.x2);
					 System.out.println(name + " is with " + area);
				 }
				 
				
				 
				 
				 
//				 for(Tuple lt : l1)
//				 {
//					 try {
//						 System.out.print("................");
//						System.out.print(lt.getShapeFld(3).x1);
//					} catch (FieldNumberOutOfBoundException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					} catch (IOException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//					 
//				 }
				 
			 }
			 
			 
			 else if (SelectSpatial.equals("INTERSECTION"))
			 {
				 List<Tuple> t1 = tupafterrefine.get(0);
				 List<Tuple> t2 = tupafterrefine.get(1);
				 Shape [] result = new Shape[t1.size()*t2.size()];
				 List<data> tableinfo1 = CreateTableFunction.tableinfo.get(tableName[0]);
				 List<data> tableinfo2 = CreateTableFunction.tableinfo.get(tableName[1]);

				 int shapedim1 = 1;
				 for (;shapedim1<=tableinfo1.size();shapedim1++)
				 {
					 if (tableinfo1.get(shapedim1-1).type.startsWith("SDO"))
						 break;
				 }
				 
				 int shapedim2 = 1;
				 for (;shapedim2<=tableinfo2.size();shapedim2++)
				 {
					 if (tableinfo2.get(shapedim2-1).type.startsWith("SDO"))
						 break;
				 }

				
				 for (int i = 0 ; i < t1.size();i++)
					 for (int j = 0; j <t2.size();j++)
					 {
						 Shape s1 = null;
						 Shape s2 = null;
						 Shape inter = null;
						 try {
							 s1 = t1.get(i).getShapeFld(shapedim1);
						} catch (FieldNumberOutOfBoundException | IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						 try {
							 s2 = t2.get(j).getShapeFld(shapedim2);
						} catch (FieldNumberOutOfBoundException | IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						  inter = getInter(s1,s2);
						 result[i*t1.size()+j] = inter;
 				 
					 }
				 for (Shape s : result)
				 {
					 System.out.println("the intersect is (" + s.x1 + "," + s.y1 +")" +  ", (" + s.x2 + "," + s.y2 +")");
			 
		 
		     }
			 }
			 else if (SelectSpatial.equals("RELATE"))
			 {
			//	System.out.println("i am h"  + tupafterrefine.size());
				 List<Tuple> t1 = tupafterrefine.get(0);
			//	 System.out.println(t1.size());
				 List<Tuple> t2 = tupafterrefine.get(1);
				// System.out.println(t2.size());
				 int [] result = new int[t1.size()*t2.size()];
				// System.out.println(result.length);
				 List<data> tableinfo1 = CreateTableFunction.tableinfo.get(tableName[0]);
				 List<data> tableinfo2 = CreateTableFunction.tableinfo.get(tableName[1]);

				 int shapedim1 = 1;
				 for (;shapedim1<=tableinfo1.size();shapedim1++)
				 {
					 if (tableinfo1.get(shapedim1-1).type.startsWith("SDO"))
						 break;
				 }
				 
				 int shapedim2 = 1;
				 for (;shapedim2<=tableinfo2.size();shapedim2++)
				 {
					 if (tableinfo2.get(shapedim2-1).type.startsWith("SDO"))
						 break;
				 }

				
				 for (int i = 0 ; i < t1.size();i++)
					 for (int j = 0; j <t2.size();j++)
					 {
						 Shape s1 = null;
						 Shape s2 = null;
						 try {
							 s1 = t1.get(i).getShapeFld(shapedim1);
						} catch (FieldNumberOutOfBoundException | IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						 try {
							 s2 = t2.get(j).getShapeFld(shapedim2);
						} catch (FieldNumberOutOfBoundException | IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
//						 System.out.println("--------------");
//
//						 System.out.println(s1.y2);
//						 System.out.println(s2.y2);

						 int inter = relate(s1,s2);
						 result[i*t1.size()+j] = inter;
 				 
					 }
				 for (int s : result)
				 { 
					 if (s==-1)
						 System.out.println("DISJOINT");
					 else if (s==0)
						 System.out.println("Tangent".toUpperCase());

					 else 
						 System.out.println("Intersect".toUpperCase());

			//		 System.out.println("the intersect is (" + s.x1 + "," + s.y1 +")" +  ", (" + s.x2 + "," + s.y2 +")");
			 
		 
		     }

				 
			 }
			 
	}
	
	private Shape getInter(Shape sh1, Shape sh2)
	{	
		int low_1_X, low_1_Y, low_2_X, low_2_Y = -1;
		int high_1_X, high_1_Y, high_2_X, high_2_Y = -1;
		int newX1, newY1, newX2, newY2 = -1;
		
		low_1_X = Math.min(sh1.x1, sh1.x2);
		low_1_Y = Math.min(sh1.y1, sh1.y2);
		high_1_X = Math.max(sh1.x1, sh1.x2);
		high_1_Y = Math.max(sh1.y1, sh1.y2);
		
		low_2_X = Math.min(sh2.x1, sh2.x2);
		low_2_Y = Math.min(sh2.y1, sh2.y2);
		high_2_X = Math.max(sh2.x1, sh2.x2);
		high_2_Y = Math.max(sh2.y1, sh2.y2);
		
	    Rectangle r1 = new Rectangle(low_1_X, low_1_Y, high_1_X-low_1_X, high_1_Y-low_1_Y);
	    Rectangle r2 = new Rectangle(low_2_X, low_2_Y, high_2_X-low_2_X, high_2_Y-low_2_Y);
	    Rectangle r3 = r1.intersection(r2);
	    
	    newX1 = (int) r3.getX();
	    newY1 = (int) r3.getY();
	    newX2 = (int) (r3.getX() + r3.getWidth());
	    newY2 = (int) (r3.getY() + r3.getHeight());
	    
		return new Shape(newX1, newY1, newX2, newY2);
	}


	private int relate(Shape s1, Shape s2) {

		int A = s1.x1;
		int B = s1.y1;
		int C = s1.x2;
		int D = s1.y2;
		int E = s2.x1;
		int F = s2.y1;
		int G = s2.x2;
		int H = s2.y2;
		
	
	    
		
        int areaOfSqrA = (C-A) * (D-B);
        int areaOfSqrB = (G-E) * (H-F);

        int left = Math.max(A, E);
        int right = Math.min(G, C);
        int bottom = Math.max(F, B);
        int top = Math.min(D, H);

        //If overlap
        int overlap = 0;
        if(right > left && top > bottom)
             overlap = (right - left) * (top - bottom);
        
        if (overlap > 0) return 1;
        else 
        {
        	if (A==E || A== G || C == E || C == G || B == F || B == H || D == F || D == H)
        		return 0;
        	return -1;
        }

        
	}
}
	
