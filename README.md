# hadoop-wiki-pagerank

PageRank algorithm see http://infolab.stanford.edu/~backrub/google.html .
This involves several different MapReduce passes used sequentially. 
The inputs to the program are pages from the Simple English
Wikipedia. 
Compute the "importance" of various Wikipedia pages/articles as determined
by the PageRank metric.
The Simple English Wikipedia corpus is about 500 MB, spread across 200,000 files { one per
page. However, the Hadoop DFS, like the Google File System, is designed to operate effciently
on a small number of large files rather than on a large number of small files. If we were to load
the Wikipedia files into Hadoop DFS individually and then run a MapReduce process on this,
Hadoop would need to perform 200,000 file open{seek-read{close operations { which is very time
consuming. 
Instead, this will be using a pre-processed version of the Simple Wikipedia corpus in
which the pages are stored in an XML format, with many thousands of pages per file. This has
been further preprocessed such that all the data for a single page is on the same line. This makes
it easy to use the default InputFormat, which performs one map() call per line of each file it reads.
The mapper will still perform a separate map() for each page of Simple Wikipedia, but since it is
sequentially scanning through a small number of very large files, performance is much better than
in the separate-file case.
Each page of Wikipedia is represented in XML as follows:
<title> Page_Name </title>
(other fields we do not care about)
<revision optionalAttr="val">
<text optionalAttr2="val2"> (Page body goes here)
</text>
</revision>
The pages have been flattened" to be represented on a single line. So the single line looks like:
<title>Page_Name</title>(other fields)<revision optionalAttr="val"><text
optionalAttr="val2">(Page body)</text></revision>
The body text of the page also has all newlines converted to spaces to ensure it stays on one line in
this representation. Links to other Wikipedia articles are of the form \[[Name of other article]]".

MapReduce Steps
This presents the high-level procedures of what each phase of the program does. 
1. Step 1: Create Link Graph: Process individual lines of the input corpus, one at a time.
These lines contain the XML representations of individual pages of Wikipedia. Turns this into
a link graph and initial page rank values. Uses 1/N as your initial PageRank value, and assumes
the damping factor d is 0.85. N is the total pages in the corpus.
2. Step 2: Process PageRank: This component runs in main loop.
The output of this phase is directly readable as the input of this same step, so that
you can run it multiple times.
In this phase, program divides fragments of the input PageRank up among the links on
a page, and then recombines all the fragments received by a page into the next iteration of
PageRank.
3. Step 3: Cleanup and Sorting: The goal of this step is to understand which pages on
Wikipedia have a high PageRank value. Therefore, we use one more "cleanup" pass to extract
this data into a form we can inspect. The output is just a mapping between page names and PageRank
values. The output data is sorted by PageRank value.
At this point, the data can be inspected and the most highly-ranked pages can be determined.

Two very simple graphs, graph1.txt and graph2.txt, and a very small test data set wiki-
micro.txt are uploaded

INSTRUCTIONS
1. Instructions to run HADOOP page rank program
1.0 Parsing assumptions in implementation :
	1.0.1 Extracting links from page body only (as denoted by <text> tag)
	1.0.2 Discarding pattern with nested links, like [[  [[  ]]  [[ ]] ]]
	1.0.3 Page xml in each line (no blank lines)
	1.0.4 Nr of pages in corpus = nr of occurences of <title> tag
1.1 Put the required source file into "input" folder in the same location as the .jar:
	for example: hadoop fs -put /projects/cloud/pagerank/simplewiki-20150901-pages-articles-processed.xml input/
1.2 Run jar (the program read from "input" and saves to "result":
	hadoop jar ktarnows_pageRank.jar pagerank.Driver
1.3 View results:
	hadoop fs -cat result/*
or import result to local file
	hadoop fs -copyToLocal result/* /users/<username>

1.3 If you want ot run again, remove output and result directory:
	hadoop fs -rm -r output
	hadoop fs -rm -r result
