register 'CustomLoader.jar';
																																										book_record_temp = LOAD '/new_edureka/project/BX-Books.csv' using book.CustomLoaderBook('0','1','3') as (bookid:chararray, booktitle:chararray, year:int);
																																										rating_record_temp = LOAD '/new_edureka/project/BX-Book-Ratings.csv' using book.CustomLoader('0','1','2') as (userid:chararray, bookid:chararray, rating:int);

book_record = distinct book_record_temp;
rating_record = distinct rating_record_temp;

limit_book_record = limit book_record 600000;
limit_rating_record = limit rating_record 2500000;
filter_book_record = filter limit_book_record by year == 2002;
join_record_temp = join filter_book_record by bookid, limit_rating_record by bookid;
join_record = foreach join_record_temp generate $5,$0,$1,$2,$3;
Group_By_Rating = GROUP join_record by rating;
Count_By_Rating = FOREACH Group_By_Rating GENERATE COUNT(join_record), $0;
STORE Count_By_Rating INTO '/new_edureka/project/Assignment3Result.txt';