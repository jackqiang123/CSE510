package dbinterface;

public class InsertAttribute 
{
	private String type = null;
	private String attributeValue = null;
	private double x1, x2, y1, y2 = -1;
	
	
	public InsertAttribute(String attr)
	{
		this.type = "string";
		this.attributeValue = attr;
	}
	
	public InsertAttribute(String attrName, String cords)
	{
		this.type = attrName;
		String[] cords_value = cords.split(", ");
		this.x1 = Double.parseDouble(cords_value[0]);
		this.y1 = Double.parseDouble(cords_value[1]);
		this.x2 = Double.parseDouble(cords_value[2]);
		this.y2 = Double.parseDouble(cords_value[3]);	
	}
	
	public String getType()
	{
		return type;
	}
	
	public String getAttributeValue()
	{
		return attributeValue;
	}
	
	public double getX1()
	{
		return x1;
	}
	
	public double getX2()
	{
		return x2;
	}
	
	public double getY1()
	{
		return y1;
	}
	public double getY2()
	{
		return y2;
	}
}
