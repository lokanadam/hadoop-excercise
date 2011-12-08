JAR=jar
JAVAC=javac
HADOOP=hadoop

# The name of main class in Java
MAIN_CLASS=LineIndexer

# Source codes
JAVA_FILES=LineIndexer.java

# The JAR file
JAR_NAME=lineindexed.jar

# The folder that contains compiled .class files
CLASS_FOLDER=classes/

# File paths on HDFS
INPUT=/shakespeare
OUTPUT=/lineindexer/output

all:
	$(JAVAC) -d $(CLASS_FOLDER) $(JAVA_FILES)
	$(JAR) -cvf $(JAR_NAME) -C $(CLASS_FOLDER) .

run:
	$(HADOOP) jar $(JAR_NAME) $(MAIN_CLASS) $(INPUT) $(OUTPUT)

result:
	$(HADOOP) dfs -cat $(OUTPUT)/part-00000
