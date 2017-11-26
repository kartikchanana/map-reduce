//main pagerank scala class
object pagerank {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Page rank spark").setMaster("yarn")
    val sc = new SparkContext(conf)

    //use the input argument and parse every line through the bz2 parser
    //Return an rdd pagename => outlinks
    val adjacencyList= sc.textFile(args(0))
      .map(line => Bz2WikiParser.adjacencyList(line))
      .filter(line => !line.contains("invalid") && !line.isEmpty)
      .map(line => line.split("=>"))
      .map(fields =>
        if(fields.length>1) (fields(0),fields(1).split(", ").toList)
        else {(fields(0),List[String]())}).persist()

    val count = adjacencyList.count()
    //calculate initial page rank
    val initialRank = 1.0/count

    //create a map of all pages to initial page rank
    var pageranks = adjacencyList.mapValues(page => initialRank)


    //pagerank iterations
    val iter = 10
    val damp_factor = 0.15
    for(i <- 1 to iter){
      //join the two rdds and create a flatmap from the values of the resulting rdd
      //on those values run the pagerank formaula, reduce by key and map it back to the page names
      val rankIter = adjacencyList.join(pageranks)
        .flatMap{case (url, (links, rank)) =>
          val rdd = Seq((url, 0.0))
          links.map( link => (link, rank/links.size)).union(rdd)
        }
      //collect contrbutions from all pages
      pageranks = rankIter.reduceByKey(_+_)
      //calculate dangling factor for the flatmap and usi it in page rank formula
      var dangling = pageranks.lookup("").sum
      var danglingFactor = dangling/count

      //map the page name to calculated page rank including dangling factor
      var ranks = pageranks.mapValues(rank => damp_factor/count + (1-damp_factor)*(danglingFactor + rank))
    }

    //sort the page ranks in inverted order and get the array of top 100 pages from it
    val pr = pageranks.sortBy(-_._2).take(100)
    //write the top 100 results to a text file
    sc.parallelize(pr,1).saveAsTextFile(args(1))
    System.exit(0)
  }
}
