package dbinterface;

public class condition
{
	 private String left;
	 private String right;
	 
	 public condition (String s1, String s2)
	 {
		 this.left = s1;
		 this.right = s2;
	 }
	 
	 public String getLft()
	 {
		 return left;
	 }
	 
	 public String getRgt()
	 {
		 return right;
	 }
}
