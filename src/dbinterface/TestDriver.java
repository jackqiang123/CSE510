package dbinterface;

/* Co-author: Xiaoyu Zhang
 * 			  Bing Han        
 */

public class TestDriver
{
	public static void main(String[] args)
	{
		// Every test cases match the expected outputs! 
		// We'd better create a Makefile for future use!
		String test1 = "CREATE TABLE cola_markets (mkt_id NUMBER PRIMARY KEY, name VARCHAR2(32), shape SDO_GEOMETRY)";
		String test2 = "CREATE INDEX cola_spatial_idx ON cola_markets(shape) INDEXTYPE IS MDSYS.SPATIAL_INDEX;";
		String test3 = "INSERT INTO cola_markets VALUES(1, 'cola_a', SDO_GEOMETRY( 1.0, 2.0, 3.0, 4.0 ));";
		String test4 = "SELECT SDO_GEOM.SDO_INTERSECTION(c_a.shape, c_c.shape, 0.005) FROM cola_markets c_a, cola_markets c_c WHERE c_a.name = 'cola_a' AND c_c.name = 'cola_c'";
		String test5 = "SELECT c.name, SDO_GEOM.SDO_AREA(c.shape, 0.005) FROM cola_markets c WHERE c.name = 'cola_a'";
		String test6 = "SELECT SDO_GEOM.RELATE(c_b.shape, 'anyinteract', c_d.shape, 0.005) FROM cola_markets c_b, cola_markets c_d WHERE c_b.name = 'cola_b' AND c_d.name = 'cola_d'";
		
		
		Command cmd;
		cmd = Parser.createCommand(test1);
		if(cmd == null)
		{
			System.exit(1);
		}
		else
		{
			cmd.Start();
		}
		
		cmd = Parser.createCommand(test2);
		if(cmd == null)
		{
			System.exit(1);
		}
		else
		{
			cmd.Start();
		}
		
		cmd = Parser.createCommand(test3);
		if(cmd == null)
		{
			System.exit(1);
		}
		else
		{
			cmd.Start();
		}
		
		cmd = Parser.createCommand(test4);
		if(cmd == null)
		{
			System.exit(1);
		}
		else
		{
			cmd.Start();
		}
		
		cmd = Parser.createCommand(test5);
		if(cmd == null)
		{
			System.exit(1);
		}
		else
		{
			cmd.Start();
		}
		
		cmd = Parser.createCommand(test6);
		if(cmd == null)
		{
			System.exit(1);
		}
		else
		{
			cmd.Start();
		}
	}
}
