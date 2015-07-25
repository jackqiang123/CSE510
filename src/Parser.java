import java.util.HashSet;

public static class Parser {
	private static HashSet<String> comset = new HashSet<String>();

	public Parser() {
		comset.add("Create Index".toUpperCase());
		comset.add("Create Table".toUpperCase());
		comset.add("Insert".toUpperCase());
		comset.add("Select".toUpperCase());

	}

	// Method to create Command
	public Command createCommand(String cmd) {
		String[] cm = cmd.split(" ");
		String s1 = cm[0];
		String s2 = (cm[0] + " " + cm[1]).toUpperCase();
		if (!(comset.contains(s1) || comset.contains(s2))) {
			System.out.println("Wrong Commd");
			System.exit(0);
		}
		if (s1.equals("CREATE")) {
			if (s2.equals("Create Table".toUpperCase()))
				return new CreateCommand(cmd);
			else if (s2.equals("Create Index".toUpperCase()))
				return new IndexCommand(cmd);
			else {
				System.out.println("Wrong Commd");
				System.exit(0);
			}

		} else if (s2.equals("Insert into".toUpperCase()))
			return new InsertCommand(cmd);
		else if (s1.equals("SELECT"))
			return new SelectionCommand(cmd);
		else {
			System.out.println("Wrong Commd");
			System.exit(0);
		}

	}

}
