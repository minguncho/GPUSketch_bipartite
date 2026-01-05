# GPUSketch Bipartite Graph Generator
A bipartite graph generator using subreddit datasets for GPUSketch. 

## Installation
```
mkdir build
cd build
cmake ..
make
```

## Generation
```
./build/main <INPUT_DATASET>
```
Within `./datasets` directory, there are input dataset txt files (`reddit_100active_Xsub_corpus.txt`) that you can use to generate the bipartite graph. The `X` stands for the number of included subreddits out of 100 highly active subreddits. 

There are 4 main stages in the code.

1. Parsing input file.
2. Build a buffer of global edges. (Allocate memory, insert edges, sort, then remove duplicate edges)
3. Shuffling edges in global edge buffer. (I expect this step should take the most) 
4. Building a binary stream. 

This was log output for `reddit_100active_9sub_corpus.txt` on our machine:
```
Dataset has 557314 users, 9 subreddits, and 592543 interactions 
Finished parsing input file: 0.224628s 
Maximum Memory Usage(MiB): 29.25
collect_edges() num_threads: 96
  Finished initializing buffer for global edges: 91.9681s 
  Finished building global edges: 7.05966s 
  Finished sorting global edges: 81.4108s 
  Finished checking and removing duplicates: 206.2s 
Total number of edges collected: 23829133568
Size of global_edges: 23772680719. Duplicate edges: 56452849
Finished collecting edges: 386.732s 
Maximum Memory Usage(MiB): 363644
Num Nodes: 557314
Num Edges: 23772680719
Finished shuffling edges: 4186.68s 
Maximum Memory Usage(MiB): 363644
Finished converting to binary stream: 1580.32s 
Maximum Memory Usage(MiB): 363644
```