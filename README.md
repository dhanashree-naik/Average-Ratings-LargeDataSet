# Average-Ratings-LargeDataSet

Task1: (40%)
calculate each movie’s average rating based on gender of the user. 

#INPUT data:
https://grouplens.org/datasets/movielens/
- ratings.dat 
- users.dat 

1. Run this command =:   spark-submit --class "Dhanashree_Naik_task1" --master local[2] "path_to_Dhanashree_Naik_task1.jar" "path_to_users.dat" "path_to_ratings.dat"

Note: please specify the path to users.dat file and path to ratings.dat file in the same given order 

for example : spark-submit --class "Dhanashree_Naik_task1" --master local[2] target/scala-2.10/Dhanashree_Naik_task1.jar /home/dhanashree/Dhanashree_Naik_task1/src/main/scala/users.dat /home/dhanashree/Dhanashree_Naik_task1/src/main/scala/ratings.dat 

#OUTPUT task 1:
The result is ordering by movieId, gender in ascending order
The result file includes three columns movieId, gender, avg. ratings.


Task_2 :
calculate the average rating of each movie genres based on the gender of the user.
#INPUT data:
- ratings.dat, 
- movies.dat and 
- users.dat files


1. Run this command =:    spark-submit --class "Dhanashree_Naik_task1" --master local[2] "path_to_Dhanashree_Naik_task2.jar" "path_to_users.dat" "path_to_ratings.dat" "path_to_movies.dat"

Note: please specify the path to users.dat file ,path to ratings.dat file and path to movies.dat in the same given order 

for example:   spark-submit --class "Dhanashree_Naik_task2" --master local[2] target/scala-2.10/Dhanashree_Naik_task2.jar /home/dhanashree/Dhanashree_Naik_task2/src/main/scala/users.dat /home/dhanashree/Dhanashree_Naik_task2/src/main/scala/ratings.dat /home/dhanashree/Dhanashree_Naik_task2/src/main/scala/movies.dat

#OUTPIT format - task 2
There are three columns in the result file. The first column is the genres’s name.
the second column is the gender and the third column is the avg. ratings. Also,
the file should be sorted according to the genres' name in ascending order.

