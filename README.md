MELODY-Join
===========
The Hadoop based implementation of MELODY-Join algorithm presented in the paper [Jin Huang, Rui Zhang, Rajkumar Buyya, and Jian Chen, "MELODY-Join: Efficient Earth Mover's Distance Similarity Join Using MapReduce", ICDE2014.] (http://people.eng.unimelb.edu.au/huangj1/resources/icde2014_melody.pdf )

What It Does
----
It uses MapReduce/BSP parallel computation paradigm to retrieve all pairs of similar records from large datasets, i.e., to perform similarity join on datasets. The similarity is measured by the [Earth Mover's Distance(EMD)](http://en.wikipedia.org/wiki/Earth_Mover%27s_Distance). The implementation here uses Euclidean distance as the ground distance of EMD. 

Two types of joins are supported:
- **Distance threshold**: the distance between the records from the retrieved pairs is below a given value, MapReduce only
- **Top-_k_**: the retrieved pairs have the k smallest distances among all pairs from a cartesian product, MapReduce and BSP

Input Datset Format
----
Data records are represented by histograms that have multiple bins. Each bin has a non-negetive weight and a multi-dimensional location vector. As we assume all datasets have the same histogram definition, the locations of bins are shared by all data records. All data are encoded in text file, and followings are their formats:
- Histograms: each line represents one record, `<id> <weight of bin 0> <weight of bin 1> ...`
- Bin Locations: all locations in one line, `<bin 0 dimension 0> <bin 0 dimension 1> ... <bin n dimension 0> <bin n dimension 1> ...` 

As MELODY-Join exploits the projection and normal lower bounds of EMD, it requires the projection vectors in the following format:
- Projection vector: all vectors in one line, `<vector 0 dimension 0> <vector 0 dimension 1> ... <vector n dimension 0> <vector n dimension 1> ...`

All files should be accessible via HDFS paths.

How To Run
----
The implementation uses Apache Maven for dependency management. You can run `mvn clean install assembly:single` to generate the jar file. Additionally, make sure your environment meets the following requirement
- Apache Hadoop 2.0+
- Apache Hama 0.7.0-SNAPSHOT

The following dependencies should be in the `/lib` directory of the Hadoop and Hama:
- commons-math3-3.1.1
- commons-math-2.1
- commons-collections-3.2.1
- commons-configuration1.6

Before running the generated jar using Hadoop or Hama, fill the `conf.properties` file according to the provided `melody-conf.properties`.

The algorithms can be run using the command

    hadoop jar <jar file path> com.iojin.melody.Join <conf.properties>
    hama jar <jar file path> com.iojin.melody.Join <conf.properties>

Example Datasets
----
[Corel] (http://archive.ics.uci.edu/ml/datasets/Corel+Image+Features)

[Mirflickr] (http://medialab.liacs.nl/mirflickr/mirflickr1m/)

Note that you need to convert the format to the one specified above before feeding them to the program.
