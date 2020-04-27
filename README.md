# Introduction 

This project is aimed to read and split and write avro and csv  files through streaming


# Build and Test

mvn clean package

java -jar FileSplit-0.1-jar-with-dependencies.jar ${dir_name} ${option}  ${max_record_count} </br>
java -jar FileSplit-0.1-jar-with-dependencies.jar ${dir_name} ${option}  ${max_record_count} </br>

Where,

dir_name - directory name </br>
option - 1 or 2 - avro or csv </br>
max_record_count - maximum record count in each file </br>

example :

java -jar FileSplit-0.1-jar-with-dependencies.jar input1 1  75000000 </br>
java -jar FileSplit-0.1-jar-with-dependencies.jar input1 2  125000000 </br>


