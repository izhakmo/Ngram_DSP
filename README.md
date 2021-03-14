# Ngram


### Running instructions

insret the args as follow:
1. jar_path
2. output_path (bucket)
3. ec2 key pair
4. write "yes" or "no" if you want to run the program with a combiner

## Info
N-grams are fixed size tuples of items.

- Given a corpus from google three-gram, the program do as follow:

	- Counts the legal words and divide the corpus into two parts.
	- Calculate the probability of each three-gram.
	- Sort the Ngrams alphabetically, and by probability.

## Architecture

The project has 4 map reduce jobs:

1. Job 1 filters illegal three grams, divide the corpus, sums the occurrences of each three gram at each corpus, counts the number of three-grams in the corpus.

2. Job 2 calculates the occurrence of each three-gram at each corpus, and join the data

3. Job 3 mapper join the data from the 2 splits of the corpus, and calculate the probabilty of each three-gram

4. Job 4 is executing the sort - (w1w2 alphabetically ascending) and then (w1w2w3 probability descending).
