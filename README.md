# Clarisights ETL Assignment

## Assumptions

- A line of CSV <= 1 kB for flushing
- Format of data in CSV is as provided in the examples

## Setup and execution

### Creating and running the binary file

Go version `^1.16` required

1. Create the executable file
    ```bash
    $ go build main.go
    ```

2. Run the executable file
    ```bash
    $ ./main "<input filename>"
    ```
---

## Thought Process

### Overview

The objective was to group the data provided in the csv by dimensions and aggregate the metrics (summation)

The task seems very simple at first sight but after looking at the constraints provided, the problem solving method totally changes.

The major constraint that the file size can be upto 1 TB in size is the major factor to consider before designing a solution. You cannot group the data as easily because you don't have the required physical memory to load the whole file and process it.

Processing that amount of data requires to follow an approach of batch processing of the data i.e. splitting the data into multiple smaller batches and performing the operations on the smaller batches and then merging the solution of the batches to a single output.

Writing a program which can process the batches of data in parallel wherever it was possible was the main ideology during the design phase.

Data can be processed parallely on multiple levels:

1. Using multi-threaded processing to process multiple batches consecutively on a single machine.

2. Using multiple nodes to process set of batches and aggregating them finally on a single machine.

The program written is following the 1st approach, but with some tweaks we can also make use of multiple nodes to process batches of data. This can lead to even more faster processing.

### Solution Approach

To group the data the data needed to be sorted because one dimension can be present in the starting of the file and the similar dimension can also be present in the ending.

So to make the grouping easy, similar keys could be brought together by sorting.

Now sorting this huge file is not a simple task because of the limited memory so I used **external sorting** algorithm.

Steps followed to sort the file are:

1. Split the input file into multiple smaller chunks.
2. Sort the smaller chunks by loading them in the memory and using any sorting algorithm, I used the in built sorting function provided by Go.
3. This sorting step for multiple chunks can be performed parallely as the chunks are independent of each other. I have used `goroutines` to run 10 sorting calls in parallel. The number 10 is decided so that indefinite chunks are not loaded into the memory together causing starvation for CPU among the threads. Also doing this would take up a lot of memory (chunkSize * number of goroutines spawned). This number can be increased according to the number of CPUs available.
4. After the sort is complete on the chunks, we perform a K sorted array merging on all the chunks to create a single sorted input file.
5. Now grouping becomes easier, we can read the file line by line without consuming a lot of memory and aggregate the metrics and writing it to the output file.
