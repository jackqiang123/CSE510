package interferce;

import java.util.ArrayList;
import java.util.List;

//CREATE TABLE cola_markets ( 
//		mkt_id NUMBER PRIMARY KEY, 
//		name VARCHAR2(32), 
//		shape SDO_GEOMETRY); 

class data{
	String name;
	String type;
	boolean isPrimary;
	public data(String name,String type,boolean is)
	{
		this.name = name;
		this.type = type;
		this.isPrimary = is;
		
	}
}

public class CreateCommand{

	//constructor
	private List<data> header;
	private String tablename;
	public CreateCommand(String comm){
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
	public void Start() {
		
	System.out.println("We have create table named " + this.tablename);	
	System.out.println("The table header is shown as follows:");
	for (data d : header)
	{
		if (d.isPrimary == false)
			System.out.println("column name is " + d.name + "; the data type is " + d.type +".");
		else
			System.out.println("column name is " + d.name + "; the data type is " + d.type + "; Primary Key.");

	}
		
	}

	public static void main(String []args)
	{
		String s = "CREATE TABLE cola_markets (mkt_id NUMBER PRIMARY KEY, name VARCHAR2(32), shape SDO_GEOMETRY)";
		CreateCommand c = new CreateCommand(s);
		c.Start();
	}
	
}
