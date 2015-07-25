package dbinterface;

import global.AttrType;
import global.RID;
import global.Shape;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.Tuple;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;

import java.io.IOException;
//Author: Xiaoyu Zhang
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;


public class InsertCommand extends Command
{
	private String init_command;
	private String tableName;
	private String commandDetail;
	private ArrayList<InsertAttribute> attr_list = new ArrayList<>();
	
	//Constructor
	public InsertCommand(String commd)
	{
		this._cmd = commd;
		this.init_command = commd;
		
		String[] temp_str = init_command.trim().split(" ");
		tableName = temp_str[2];
		
		commandDetail = init_command.substring("INSERT INTO ".length() + tableName.length() + 1, init_command.length());
		if(commandDetail.startsWith("VALUES(") && commandDetail.endsWith(");"))
		{
			int length = commandDetail.length() - 2;
			String trimmedCommand = commandDetail.substring(7,length);
			
			String[] attributeComponents = new String[10];
			int flag = 0;
			boolean isLftParenEnclosed = false;
			int breakpoint = 0;
			for(int i=0; i<trimmedCommand.length(); i++)
			{
				if((trimmedCommand.charAt(i) == ',') && (isLftParenEnclosed == false))
				{
					attributeComponents[flag] = trimmedCommand.substring(breakpoint, i).trim();
					breakpoint = i+1;
					flag++;
					continue;
				}
				if(trimmedCommand.charAt(i) == '(')
				{
					isLftParenEnclosed = true;
				}
				else if(trimmedCommand.charAt(i) == ')')
				{
					isLftParenEnclosed = false;
				}
			}
			attributeComponents[flag] = trimmedCommand.substring(breakpoint, trimmedCommand.length()).trim();
			flag++;
			
			for(int j=0; j<flag;j++)
			{
				if(attributeComponents[j].contains("("))
				{
					//x1, y1, x2, y2
					String str = attributeComponents[j];
					String attrName = null;
					int _breakpoint = 0;
					for(int i=0;i<str.length();i++)
					{
						if(str.charAt(i) == '(')
						{
							attrName = str.substring(_breakpoint, i).trim();
							_breakpoint = i+1;
						}
					}
					String cvector = str.substring(_breakpoint, str.length()-1).trim();
					
					attr_list.add(new InsertAttribute(attrName, cvector));
				}
				else
				{
					attr_list.add(new InsertAttribute(attributeComponents[j]));
				}
			}
		}
	}
	
	//Methods
	@Override
	public void Start() {
		// TODO Auto-generated method stub
		
		
		Heapfile f = null;
		try {
			f = new Heapfile(this.tableName);
		} catch (HFException | HFBufMgrException | HFDiskMgrException
				| IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		List<data> l = CreateTableFunction.tableinfo.get(this.tableName);
		
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
		 try {
				t.setHdr((short) fldNum, mtype, Msizes);
			} catch (Exception e) {
				System.err.println("*** error in Tuple.setHdr() ***");
			    status = false;
				e.printStackTrace();
			}
		 
		
		try {
					for (int i = 0; i<fldNum;i++)
			{

				if (mtype[i].attrType == AttrType.attrInteger)
					
				{	
				t.setIntFld(i+1,Integer.parseInt(attr_list.get(i).getAttributeValue()));
				
				
				}
				else if (mtype[i].attrType == AttrType.attrString)
					t.setStrFld(i+1, attr_list.get(i).getAttributeValue());
				else if (mtype[i].attrType == AttrType.attrReal)
				{	
				t.setFloFld(i+1, Float.parseFloat(attr_list.get(i).getAttributeValue()));}
				else { // current filed is shape
					int x1 = (int) attr_list.get(i).getX1();
					int x2 = (int) attr_list.get(i).getX2();
					int y1 = (int) attr_list.get(i).getY1();
					int y2 = (int) attr_list.get(i).getY2();

					t.setShapeFld(i+1, new Shape(x1,y1,x2,y2));
				}

			}
	
		} 
		catch (Exception e) 
		{
			System.err
					.println("*** Heapfile error in Tuple.setStrFld() ***");
			status = false;
			e.printStackTrace();
		}
		
		RID rid = new RID();
		
		try 
		{
			rid = f.insertRecord(t.returnTupleByteArray());
		} 
		catch (Exception e) 
		{
			System.err.println("*** error in Heapfile.insertRecord() ***");
			status = false;
			e.printStackTrace();
		}
		
//		FldSpec [] Sprojection = new FldSpec[l.size()];
//		for(int i=0;i<fldNum;i++)
//		{
//	    Sprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
//		}
//		
//		 FileScan am = null;
//		    try {
//		      am  = new FileScan(this.tableName+".in", mtype, Msizes, 
//						  (short) fldNum, (short) fldNum,
//						  Sprojection, null);
//		    }
//		    catch (Exception e) {
//			      System.err.println (""+e);
//		    }
//		
//		    System.out.println("i am checking reading");
//		  			try {
//		  			Tuple t1 = am.get_next();
//		  		//	if (t1 == null) System.out.println("empty tul");
//			System.out.println(t1.getIntFld(1)+ " ---- " + t1.getStrFld(2));
//			Shape a = t1.getShapeFld(3);
//			System.out.println(a.x1+" "+a.x2+" "+a.y1+" "+a.y2);
//			} catch (Exception e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			} 		    
		
        //print details of insert data
		/*
		int listNum = attr_list.size();
		for(int i=0; i<listNum; i++)
		{
			if(attr_list.get(i).getType() == "string")
			{
				System.out.print(attr_list.get(i).getAttributeValue()+", ");
			}
			else
			{
				System.out.print(attr_list.get(i).getType()+"(");
				System.out.print(attr_list.get(i).getX1()+", "+attr_list.get(i).getY1()+", "+attr_list.get(i).getX2()+", "+attr_list.get(i).getY2()+")");
				System.out.println();
			}
		}
		*/
	}
}
