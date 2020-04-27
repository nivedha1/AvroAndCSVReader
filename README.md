# Introduction 

This project is aimed to read and split and write avro and csv  files through streaming


# Build and Test

mvn clean package

java -jar FileSplit-0.1-jar-with-dependencies.jar ${dir_name} ${option}  ${max_record_count}
java -jar FileSplit-0.1-jar-with-dependencies.jar ${dir_name} ${option}  ${max_record_count}

Where,

dir_name - directory name
option - 1 or 2 - avro or csv
max_record_count - maximum record count in each file

example :

java -jar FileSplit-0.1-jar-with-dependencies.jar input1 1  75000000
java -jar FileSplit-0.1-jar-with-dependencies.jar input1 2  125000000


