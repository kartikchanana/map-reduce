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

#### Pre-Processing Summary
Brief discussion of pre-processing steps:
1. Build a standalone program to parse input files and display them in human-readable form
(optional but strongly recommended).
2. Incorporate parser to filter out relevant links and strip URLs to only the page name.
3. Discard pages with names containing ~ as well as links containing these characters.
4. Incorporate the remaining pages and links to create a graph in adjacency list representation.

#### Overall Workflow Summary
Design all steps of the workflow as MapReduce jobs that are executed in sequence from a single driver
program:
1. Pre-processing Job: As stated above, turn input Wikipedia data into a graph represented as
adjacency lists. Make sure that only the page names are used as node and link IDs; strip off any
path information from the beginning as well as the .html suffix from the end, leaving only the
page name, and discard any pages and links containing a tilde (~) character.
2. PageRank Job: run 10 iterations of PageRank. Try to find a way to estimate how much the
PageRank values are converging. (start in the middle of week 1, complete in week 2)
3. Top-k Job: From the output of the last PageRank iteration, find the 100 pages with the highest
PageRank and output them, along with their ranks, from highest to lowest. (complete in week 2)
For PageRank, make sure your program sets the initial PageRank values to 1/numberOfPages and
handles dangling nodes. If a page appears in a link, but the page itself does not exist in the data, you can
treat it as a dangling node. This means that you do not need to remove the links pointing to it and that it
needs to be accounted for in numberOfPages.
