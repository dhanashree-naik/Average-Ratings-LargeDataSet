import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io._
import java.text.DecimalFormat


object Dhanashree_Naik_task1 {
  
  
  
  def Construct_data_mapping_1(data:String): Map[String, String] = {
		 val list = data.split("::")
		 val mapped_data = Map[String, String](
		 ( list(0) -> list(1)) 
		 )
		return mapped_data
		}
  def Construct_data_mapping_6(data:String): Map[String, String] = {
		
		 val list = data.split("::")
		 
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
  def Construct_data_mapping_3(movieId:String, gender:String, rating:String ): Map[(String, String),Double] = {
		
		 
		 //Creating the Map of values
		 val mapped_data = Map[(String, String),Double](
		 ( (movieId , gender) ->  rating.toDouble )
		 )
		return mapped_data
		}
  def Construct_data_mapping_4(gender:String, movieId:String, avg_rating:Double ): Map[(String, String),Double] = {
		
		 
		 //Creating the Map of values
		 val mapped_data = Map[(String, String),Double](
		 ( (gender, movieId ) ->  avg_rating )
		 )
		return mapped_data
		}
  def main(args: Array[String]) {
    
    val usersFile = args(0)
    
    val ratingFile = args(1)
    
    val conf = new SparkConf().setAppName("data mining").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val fom = new DecimalFormat("#.###########")
    val usersFileLines = sc.textFile(usersFile , 2).cache()
    val ratingFileLines = sc.textFile(ratingFile , 2).cache()
    val user_words = usersFileLines.flatMap(x => Construct_data_mapping_1(x))
    val rating_words = ratingFileLines.flatMap(x => Construct_data_mapping_2(x))
    //rating_words.foreach(f=>println(f))
    val joined_maps = rating_words.join(user_words)
    //joined_maps.foreach(f=>println(f))
    //for ((k,v) <- joined_maps) {println(v) }  
    val finalMap = joined_maps.values.flatMap(f=>Construct_data_mapping_3(f._1 ._1 ,f._2 ,f._1 ._2 ))
    //finalMap.foreach(f=>println(f))  
    val group = finalMap.groupByKey    
    //group.values.foreach(f=>println(f.sum/f.size.toFloat))
    //group.foreach(f=>println(f))
    //val final_ans = group.foreach(f=> Construct_data_mapping_4(f._1 ._2 ,f._1 ._1 ,f._2 .sum/f._2 .size.floatValue)  )
    
    //print(group.getClass())
	//for ((k,v) <- group) { println(k.toString,  (v.sum/v.size.toFloat).toString) } 

    val newArray = group.collect()
    //newArray.foreach(f=>println(f))
    val final_ans = newArray.flatMap(f=>Construct_data_mapping_4(f._1 ._1 ,f._1 ._2 ,(f._2 .sum/f._2 .size)))
    
    val fin = final_ans.toList.sortBy(f=>(f._1._1 .toInt, f._1 ._2 ))
    //val fin = final_ans.map(x => x._1._1  + "," + x._1 ._2  + "," + x._2)
    //fin.foreach(f=>println(f))
    new PrintWriter("Dhanashree_Naik_Result_Task1.txt") { write(fin.foreach(f=>println(f._1._1 +","+f._1 ._2 +","+fom.format(f._2))).toString()); close }

  }
  
  
}
