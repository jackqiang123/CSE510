package dbinterface;

// Author: Lian Lu
//CREATE TABLE cola_markets ( 
//mkt_id NUMBER PRIMARY KEY, 
//name VARCHAR2(32), 
//shape SDO_GEOMETRY); 

import heap.Heapfile;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class CreateCommand extends Command
{	
	
	public List<data> header;
	public String tablename;
	
	//constructor
	public CreateCommand(String comm)
	{
		this._cmd = comm;
		String[]com = comm.split(" ");
		if ((com[0]+com[1]).toUpperCase().equals("CREATETABLE") == false || com.length <= 3)
		{
			System.out.println("wrong branches!"); // throw exception}
		}
		
		this.tablename = com[2]; // store the table name
		
		int startindex = 0;
		while(comm.charAt(startindex) != '(') startindex++;
		startindex++;
		String attr = comm.substring(startindex);
		String[]attrs = attr.split(",");
		header = new ArrayList<data>();
		for (String a : attrs)
		{
			a = a.trim();
			String []temp = a.split(" ");
			if (temp.length == 2)
			{
				header.add(new data(temp[0],temp[1],false));
			}
			else 	
				header.add(new data(temp[0],temp[1],true));
				
		}
				
		
	}
	
	
	
	//@Override
	public void Start()
	{
		
		CreateTableFunction glo = new CreateTableFunction();
		
		Heapfile f = null; 
		
	    f = glo.CreateTable(this.tablename, this.header);

		/*		
		System.out.println();		
		System.out.println(this._cmd);
		System.out.println("\tCreate Table: " + this.tablename);	
		System.out.println("\tAttributes:");
		for (data d : header)
		{
			if (d.isPrimary == false)
				System.out.println("\t\t column name is " + d.name + "; the data type is " + d.type +".");
			else
				System.out.println("\t\t column name is " + d.name + "; the data type is " + d.type + "; Primary Key.");
	
		}
		*/
	}
}
