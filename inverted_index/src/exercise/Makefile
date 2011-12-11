JAR=jar
JAVAC=javac
HADOOP=hadoop

# The name of main class in Java
MAIN_CLASS_1=exercise.InvertIndex
MAIN_CLASS_2=exercise.QueryWord

# Source codes
JAVA_FILES=InvertIndex.java QueryWord.java

# The JAR file
JAR_NAME=inverted.jar

# The folder that contains compiled .class files
CLASS_FOLDER=classes/

# File paths on HDFS
INPUT_1=/shakespeare
OUTPUT_1=/inverted/output
INPUT_2=$(OUTPUT_1)/part-00000
OUTPUT_2=/inverted/tmpOutput

all:
	$(JAVAC) -d $(CLASS_FOLDER) $(JAVA_FILES)
	$(JAR) -cvf $(JAR_NAME) -C $(CLASS_FOLDER) .

run1:
	$(HADOOP) jar $(JAR_NAME) $(MAIN_CLASS_1) $(INPUT_1) $(OUTPUT_1)

run2:
	$(HADOOP) jar $(JAR_NAME) $(MAIN_CLASS_2) $(INPUT_2) $(OUTPUT_2)

result:
	$(HADOOP) dfs -cat $(OUTPUT_1)/part-00000
