# COMP90024 Assignment 1

#### The Happiest City

### Problem Description

Your task in this programming assignment is to implement a simple, parallelized application leveraging the
University of Melbourne HPC facility SPARTAN. Your application will use a large Twitter dataset, a grid/mesh for
Melbourne and a simple dictionary of terms related to sentiment scores. Your objective is to calculate the sentiment
score for the given cells and hence to calculate the area of Melbourne that has the happiest/most miserable people!

### Versions

  - v1.0 - Aho Corasick & master collects from slaves using p2p communication
  - v1.1 - Aho Corasick & collection of results is done using mpi.gather 

### Overview

The main entry point is in run.py.
run.py is called by each worker and the resulting process behaviour is dependent on whether the worker is a master or a slave.
The data file, twitter.json, is divided into even-sized chunks and each chunk is assigned to a worker.
The master process then collects and aggregates the results of this processing and returns the result.


'AFINN.txt' contains a dictionary of words and their asossicated sentiment score (i.e. abandon -2)
The sentiment for a given tweet is calculated as the number of words in AFINN.txt that exactly match strings in the tweet.
Strings with punctuation at the end are still considered an exact match - i.e. by this definition, 'cheese' is a match in the string 'i like cheese!'  


Pattern matching is implemented using a modified version of **Aho-Corasick**. As we only need to construct the autmaton once, Aho-Corasick allows us to perform the calculation of a tweet's sentiment in O(n + z) time; where **n** is the _length_ of the tweet and **z** is the total number of occurences of words from AFINN.txt in the tweet. This enables the performance of this implementation to be extremely fast. 


### Quickstart

To run this code locally, first ensure you have some form of mpi installed (i.e. openMPI, mpich)
On linux this can be done easily through a package manager:


```bash
sudo apt install mpich
```

Then you can run the program as follows:

```bash
mpiexec -np <num processors> python3 run.py <Path/To/twitter.json> 
```
