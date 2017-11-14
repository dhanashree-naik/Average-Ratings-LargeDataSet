import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io._
import java.text.DecimalFormat



object Dhanashree_Naik_task2 {

  
  def Construct_data_mapping_1(data:String): Map[String, String] = {
		
		 val list = data.split("::")
		 //Creating the Map of values
		 val mapped_data = Map[String, String](
		 ( list(0) -> list(1)) 
		 )
		return mapped_data
		}
  def Construct_data_mapping_6(data:String): Map[String, String] = {
		
		 val list = data.split("::")
		 //Creating the Map of values
		 val mapped_data = Map[String, String](
		 ( list(0) -> list(2)) 
		 )
		return mapped_data
		}
  def Construct_data_mapping_2(data:String): Map[String, (String,String)] = {
		
		 val list = data.split("::")
		 //Creating the Map of values
		 val mapped_data = Map[String, (String,String)](
		 ( list(0) -> (list(1) , list(2)) )
		 )
		return mapped_data
		}
  def Construct_data_mapping_3(movieId:String, gender:String, rating:String ): Map[(String), (String,Double)] = {
		
		 
		 //Creating the Map of values
		 val mapped_data = Map[(String), (String,Double)](
		 ( movieId -> (gender, rating.toDouble ))
		 )
		return mapped_data
		}
  
  def Construct_data_mapping_4(genre:String, gender:String, avg_rating:Double ): Map[(String, String),Double] = {
		
		 
		 //Creating the Map of values
		 val mapped_data = Map[(String, String),Double](
		 ( (genre, gender ) ->  avg_rating )
		 )
		return mapped_data
		}
  
  def Construct_data_mapping_7(genre:String, gender:String, rating:Double ): Map[(String, String),Double] = {
		
		 
		 //Creating the Map of values
		 val mapped_data = Map[(String, String),Double](
		 ( (genre, gender ) ->  rating )
		 )
		return mapped_data
		}
  def main(args: Array[String]) {
    val users_File = args(0)
    val rating_File = args(1)
    val Movie_File = args(2)
    val conf = new SparkConf().setAppName("data mining").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val fom = new DecimalFormat("#.###########")
    val users_FileLines = sc.textFile(users_File , 2).cache()
    val rating_FileLines = sc.textFile(rating_File , 2).cache()
    val Movies_FileLines = sc.textFile(Movie_File, 2).cache()
    val user_words = users_FileLines.flatMap(x => Construct_data_mapping_1(x))
    //user_words.foreach(f=>println(f))
    val rating_words = rating_FileLines.flatMap(x => Construct_data_mapping_2(x))
    //rating_words.foreach(f=>println(f))
    
    val joined_maps = rating_words.join(user_words)
    //joined_maps.foreach(f=>println(f))
    
    //for ((k,v) <- joined_maps) {println(v) }  

    val finalMap = joined_maps.values.flatMap(f=>Construct_data_mapping_3(f._1 ._1 ,f._2 ,f._1 ._2 ))
    
    
    val movies_words = Movies_FileLines.flatMap(x => Construct_data_mapping_6(x))
    //movies_words.foreach(f=>println(f))
    
    val new_join = movies_words.join(finalMap)
    //new_join.foreach(f=>println(f))
    
    val new_map = new_join.values.flatMap(f=> Construct_data_mapping_4(f._1 ,f._2._1 ,f._2 ._2 ))
    //new_map.foreach(f=>println(f))
    
    val group = new_map.groupByKey
    //group.foreach(f=>println(f))
    val newArray = group.collect()
    
    val final_ans = newArray.flatMap(f=>Construct_data_mapping_4(f._1 ._1 ,f._1 ._2 ,(f._2 .sum/f._2 .size)))
    //final_ans.foreach(f=>println(f))
    val fin = final_ans.toList.sortBy(f=>(f._1._1,f._1._2 ))
    //fin.foreach(f=>println(f))
    new PrintWriter("Dhanashree_Naik_Result_task2.txt") { write(fin.foreach(f=>println(f._1._1 +","+f._1 ._2 +","+fom.format(f._2))).toString()); close }
  }
  
  
}
