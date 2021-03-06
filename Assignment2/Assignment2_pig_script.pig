register 'CustomLoader.jar';

book_record_temp = LOAD '/new_edureka/project/BX-Books.csv' using book.CustomLoader('0','1','2','3','4','5','6','7') as (bookid:chararray, booktitle:chararray, author:chararray, year:int, publisher:chararray, imageurls:chararray, imageurlm:chararray, imageurll:chararray);
book_record = distinct book_record_temp;
filter_record = filter book_record by year != 0;
Group_By_Year = GROUP filter_record by year;
Count_By_Year = FOREACH Group_By_Year GENERATE COUNT(filter_record), $0;
sorted_result = ORDER Count_By_Year by $0 desc;
store sorted_result into '/new_edureka/project/Assignment2Result.txt';
