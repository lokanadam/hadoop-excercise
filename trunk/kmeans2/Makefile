all: kmeans.jar

kmeans.jar : src/DataPrepare.java src/Canopy.java src/KMeans.java src/KMeansIter.java src/MarkData.java src/Viewer.java
	rm -rf classes
	mkdir classes
	javac -d classes src/*.java
	jar -cvf kmeans.jar -C classes .

clean :
	rm -rf classes
	rm kmeans.jar
