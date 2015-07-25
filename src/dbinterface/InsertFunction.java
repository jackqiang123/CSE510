package dbinterface;

import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;

import java.io.IOException;
import java.util.List;

public class InsertFunction {
public void insert(String table,List<InsertAttribute> list)
{
	Heapfile f = null;
			try {
				f = new Heapfile(table+".in");
			} catch (HFException | HFBufMgrException | HFDiskMgrException
					| IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			List<data> tableinfo = CreateTableFunction.tableinfo.get(table+".tb");	
			System.out.println(tableinfo==null);
}
}
