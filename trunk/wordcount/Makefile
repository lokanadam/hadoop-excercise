JAR=jar
JAVAC=javac
HADOOP=hadoop

# The name of main class in Java
MAIN_CLASS=WordCount

# Source codes
JAVA_FILES=WordCount.java

# The JAR file
JAR_NAME=wordcount.jar

# The folder that contains compiled .class files
CLASS_FOLDER=classes/

# File paths on HDFS
INPUT=/wordcount/shakespeare.tar.bz2
OUTPUT=/wordcount/output

all:
	$(JAVAC) -d $(CLASS_FOLDER) $(JAVA_FILES)
	$(JAR) -cvf $(JAR_NAME) -C $(CLASS_FOLDER) .

run:
	$(HADOOP) jar $(JAR_NAME) $(MAIN_CLASS) $(INPUT) $(OUTPUT)

result:
	$(HADOOP) dfs -cat $(OUTPUT)/part-00000
