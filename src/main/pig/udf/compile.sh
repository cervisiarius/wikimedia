#/bin/sh

javac -cp /usr/lib/pig/pig-0.12.0-cdh5.3.1.jar org/wikimedia/west1/pigudf/*.java
jar -cf pigudf.jar org
