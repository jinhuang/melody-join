Melody-Join
===========
The Hadoop based implementation of Melody-Join algorithm presented in the paper [Jin Huang, Rui Zhang, Rajkumar Buyya, and Jian Chen, "Melody-Join: Efficient Earth Mover's Distance Similarity Join Using MapReduce", ICDE2014.] (http://people.eng.unimelb.edu.au/huangj1/resources/icde2014_melody.pdf )

What It Does
----
It uses MapReduce/BSP parallel computation paradigm to retrieve all pairs of similar records from large datasets, i.e., to perform similarity join on datasets. The similarity is measured by the [Earth Mover's Distance(EMD)](http://en.wikipedia.org/wiki/Earth_Mover%27s_Distance). The implementation here uses Euclidean distance as the ground distance of EMD. 

Two types of joins are supported:
- **Distance threshold**: the distance between the records from the retrieved pairs is below a given value, MapReduce only
- **Top-_k_**: the retrieved pairs have the k smallest distances among all pairs from a cartesian product, MapReduce and BSP

The [MRSimJoin] (http://www.public.asu.edu/~ynsilva/SimCloud/publications.html) implementation released by the original authors is modified to generically process arbitrary dimensional datasets. That code is under the package `mrsim`.

Additionally, it provides a data generator which extracts content-based features from image files, which can be fed to the join algorithm to process. The feature extraction is provided by [Lire] (http://www.semanticmetadata.net/lire/). 

Input Datset Format
----
Data records are represented by histograms that have multiple bins. Each bin has a non-negetive weight and a multi-dimensional location vector. As we assume all datasets have the same histogram definition, the locations of bins are shared by all data records. All data are encoded in text file, and followings are their formats:
- Histograms: each line represents one record, `<id> <weight of bin 0> <weight of bin 1> ...`
- Bin Locations: all locations in one line, `<bin 0 dimension 0> <bin 0 dimension 1> ... <bin n dimension 0> <bin n dimension 1> ...` 

As Melody-Join exploits the projection and normal lower bounds of EMD, it requires the projection vectors in the following format:
- Projection vector: all vectors in one line, `<vector 0 dimension 0> <vector 0 dimension 1> ... <vector n dimension 0> <vector n dimension 1> ...`

All files should be accessible via HDFS paths.

How To Run Join
----
The implementation uses Apache Maven for dependency management. Additionally, make sure your environment meets the following requirement
- Apache Hadoop 2.0+
- Apache Hama 0.7.0-SNAPSHOT

Then you can run `mvn clean assembly:single` to generate the jar file.

The following dependencies should be in the `/lib` directory of the Hadoop and Hama:
- commons-math3-3.1.1
- commons-math-2.1
- commons-collections-3.2.1
- commons-configuration1.6

Before running the generated jar using Hadoop or Hama, fill the `conf.properties` file according to the provided `melody-conf.properties`.

The algorithms can be run using the command

    hadoop jar <jar file path> com.iojin.melody.Join <conf.properties>
    hama jar <jar file path> com.iojin.melody.Join <conf.properties>

How To Run Generator
----
The implementation includes a data generator which extract typical content-based features from images files and convert their format to fit the requirement of the join program. The configurations are also included in the `melody-conf.properties` file. It has three running modes, local file system, HDFS file system, and MapReduce (unavailable and in progress). The command for the local and HDFS modes is 

    java -jar <jar file path> com.iojin.melody.Generate <conf.properties>

And the command for the MapReduce mode is 
    
    hadoop jar <jar file path> com.iojin.melody.Generate <conf.properties>


Example Datasets
----
[Corel] (http://archive.ics.uci.edu/ml/datasets/Corel+Image+Features)

[Mirflickr] (http://medialab.liacs.nl/mirflickr/mirflickr1m/)

Note that you need to convert the format to the one specified above before feeding them to the program.
