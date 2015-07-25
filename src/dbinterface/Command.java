package dbinterface;

//Abstract class represents parent command
public abstract class Command 
{	
	//complete command
	protected String _cmd;

	//command to start execution of command
	abstract void Start();
	
}
