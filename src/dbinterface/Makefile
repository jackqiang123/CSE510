# modify the paths appropriately
JDKPATH = /usr/lib/jvm/java-7-openjdk-i386
LIBPATH = /home/brian/workspace/minjava/javaminibase/cse510-team_sage/src
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)


#testing DBInterface
#COMPILE=javaFiles



Cse510_TestDriver: Cse510_TestDriver.java
	$(JAVAC) *.java

dbinterface: Cse510_TestDriver
	$(JAVA) dbinterface.Cse510_TestDriver
	

	
	


