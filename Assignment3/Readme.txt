Used Pig script to resolve this assignment

Custom Loader program - CustomLoader.java & CustomLoaderBook.java

As the Data was not clean(there are ; in the Book title) i had to use ";" as the delimiter for loading the records. For this i had to use a custom load (UDF for Pig) 
CustomLoader.java  for BX-Book.csv
CustomLoaderBook.java  for BX-Book_Rating.csv 

These programs are present in CustomLOader.jar

The Result is stored in Assignment3Result.txt


The pig script required Limit function. Without Limit function error was thrown while execution(log attached in pig_1426599020434.log)
