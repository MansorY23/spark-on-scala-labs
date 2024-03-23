import scala.io.BufferedSource
import scala.io.Source.fromFile
import scala.collection.immutable.ListMap
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import java.io._


object intro {
  def main(args: Array[String]): Unit = {
    val source: BufferedSource = fromFile("ml-100k/u.data")
    val lines: Seq[Array[String]] = source.getLines.toList //получаем List строк, в которых значения разделены табуляциями
      .map(string => string.split("\t")) //Seq(Array(196, 242, 3, 881250949), Array(186, 302, 3, 891717742), ...)
    source.close()

    val film_300_scores = ListMap(lines.filter(_(1) == "300").
      groupBy(_(2)).
      mapValues(_.size).
      toSeq.sortBy(_._1):_*)

    val all_fims_scores = ListMap(lines.groupBy(_(2)).
      mapValues(_.size).
      toSeq.sortBy(_._1):_*)

    val list_all_movie = all_fims_scores.values.toList
    val list_300_movie = film_300_scores.values.toList

    val json_new = List(list_all_movie, list_300_movie)
    val ready_json_compact = compact(json_new)

    //создаем файл
    val file: File = new File("lab01.json")
    //создаем writer для записи файла
    val writer: BufferedWriter = new BufferedWriter(new FileWriter(file))
    //пишем строку в файл
    writer.write(ready_json_compact)
    //освобождаем ресурсы writer
    writer.close()
  }
}