# Average-Ratings-LargeDataSet
Task_1 :

1. Run this command =:   spark-submit --class "Dhanashree_Naik_task1" --master local[2] "path_to_Dhanashree_Naik_task1.jar" "path_to_users.dat" "path_to_ratings.dat"

Note: please specify the path to users.dat file and path to ratings.dat file in the same given order 

for example : spark-submit --class "Dhanashree_Naik_task1" --master local[2] target/scala-2.10/Dhanashree_Naik_task1.jar /home/dhanashree/Dhanashree_Naik_task1/src/main/scala/users.dat /home/dhanashree/Dhanashree_Naik_task1/src/main/scala/ratings.dat 


Task_2 :

1. Run this command =:    spark-submit --class "Dhanashree_Naik_task1" --master local[2] "path_to_Dhanashree_Naik_task2.jar" "path_to_users.dat" "path_to_ratings.dat" "path_to_movies.dat"

Note: please specify the path to users.dat file ,path to ratings.dat file and path to movies.dat in the same given order 

for example:   spark-submit --class "Dhanashree_Naik_task2" --master local[2] target/scala-2.10/Dhanashree_Naik_task2.jar /home/dhanashree/Dhanashree_Naik_task2/src/main/scala/users.dat /home/dhanashree/Dhanashree_Naik_task2/src/main/scala/ratings.dat /home/dhanashree/Dhanashree_Naik_task2/src/main/scala/movies.dat
