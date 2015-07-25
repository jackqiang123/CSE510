package dbinterface;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;

public class CreateTableFunction{// must be intialzied when system runs
	public static HashMap<String,List<data>> tableinfo = null;
	
	public CreateTableFunction()
	{
		 if (tableinfo == null)
			 tableinfo = new HashMap<String,List<data>>();

	}
 public Heapfile CreateTable(String tablename, List<data> list)
 {
	 tableinfo.put(tablename, list);
	 Heapfile f = null;
	try {
		//System.out.println(tablename);
		f = new Heapfile(tablename);
	} catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	//System.out.println(f.toString());
	 return f;
	 
 }
}
