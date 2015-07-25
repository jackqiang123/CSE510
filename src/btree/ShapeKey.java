package btree;

import global.Shape;

public class ShapeKey extends KeyClass {

	private Shape _shape;
	
	
	public ShapeKey(Shape shape)
	{
		setKey(shape);
	}
	
	
	public String toString()
	{
		return _shape.toString();
	}
	
	
	//getter
	public Shape getKey()
	{
		return _shape;
	}
	
	//setter
	public void setKey(Shape shape)
	{
		_shape = shape;
	}
	
	
}
