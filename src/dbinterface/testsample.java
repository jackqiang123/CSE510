package dbinterface;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import global.*;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import tests.TestDriver;
public class testsample extends tests.TestDriver implements GlobalConst
{
public static void main(String[]args)
{
	SystemDefs sysdef = new SystemDefs(dbpath, 1000, NUMBUF, "Clock");

	CreateTableFunction glo = new CreateTableFunction();
	List<data> c= new LinkedList<data>();
	c.add(new data("int","age",false));
	glo.CreateTable("sailars.in", c);
//	Heapfile f = null;
//	try {
//		f = new Heapfile("xiao.tb");
//	} catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
//		// TODO Auto-generated catch block
//		e.printStackTrace();
//	}
//	System.out.println(f.toString());

}
}
