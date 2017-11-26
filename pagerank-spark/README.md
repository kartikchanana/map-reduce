# PageRank in MapReduce

### Requirements
We are going to apply PageRank to Wikipedia articles in order to find the most
referenced ones. In practice this requires several non-trivial pre-processing steps. For example, the
Wikipedia dumps might be compressed in a format not (well-) supported by Hadoop. In principle it is
easy to find hyperlinks in HTML, doing so requires some experience with document parsers. As some
links are not interesting, an understanding of the domain document schema is necessary but takes a
significant amount of time. Dealing with such obstacles is part of the real data analysis experience, therefore
We transformed the original Wikipedia 2006 data dump into Hadoop-friendly bz2-compressed file.

#### .bz2 File Format
The bz2 compression format works well for parallel computation, because it reduces size while
remaining “splittable”, meaning that it can be processed in chunks in parallel like uncompressed data.
Other compression formats such as zip require centralized decompression. Each line of the bz2 file is
formatted to contain the name of the Wikipedia page, a colon (:), and then the full contents of the page
on a single line.
For example:
```
<page-name0>:<wikipedia file html contents0>
<page-name1>:<wikipedia file html contents1>
...
<page-namen>:<wikipedia file html contentsn>
```

#### Wikipedia HTML Format
You need to convert the Wikipedia data into an adjacency-list based graph representation, using the
original page names as the node IDs. The distracting nuisance components (e.g., file path prefix and
.html suffix) must be removed. The Wikipedia files are in XHTML so they may be parsed by either an
HTML or XML parser. An example parser will be made available together with this assignment to help
you get started. Feel free to use and modify it. It is important to only keep hyperlinks from within the
bodyContent div tag of the document and ignore all others.
For example:
```
<!DOCTYPE ...>
<html>
<a href=”url”> links in this part of page ...
 <div id=”bodyContent”>
    <a href=”URL”> links in this section, including any nested in sub <div> tags ...
 </div>
<a href=”url”> links in this part of page ...
</html>
```
From each anchor tag href attribute within the body content div, you must parse the referenced page
name. Strip off any path information from the beginning as well as the .html suffix from the end, leaving 
only the page name. Also, discard any page names containing a tilde (~) character as these are wellconnected
but uninteresting to the results.
A SAX XML parser, also using regular expressions, is provided as a usable example. You may incorporate
it into your preprocessing stage, however it is not warranted to be bug free. Basically, it is an
implementation that filters and keeps relevant links from a Wikipedia page, per the above specification.
The parser should work fine, but is not guaranteed to be 100% correct. Check its output on some
carefully selected examples to see if it performs satisfactorily. 

#### Overall Workflow Summary
Design all steps of the workflow as MapReduce jobs that are executed in sequence from a single driver
program:
1. Read the bz2-compressed input.
2. Call the input parser on each line of this input to create the graph.
3. Run 10 iterations of PageRank on the graph. The PageRank algorithm has to be written in Scala.
As before, it has to deal with dangling nodes.
4. Output the top-100 pages with the highest PageRank and their PageRank values, in decreasing
order of PageRank.
When designing your Spark program, think carefully about using RDDs, pair RDDs or DataSets. If in
doubt, try different versions and see what works best. Also think carefully about persisting data in
memory and how to best separate data that does not change from data that does. However, you do not
need to explore balanced min cut or similar algorithms that attempt to find a graph partitioning that
minimizes the number of edges between the partitions in order to minimize network traffic during
PageRank computation iterations.
