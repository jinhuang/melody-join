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

Additionally, it provides a **data generator** which is capable to extract 16 types of content-based features from image files. The generated features then can be fed to the join algorithm to process. The feature extraction implementation is built upon [Lire] (http://www.semanticmetadata.net/lire/). The supported features are listed in [Supported Features] (https://github.com/jinhuang/melody-join#supported-features).

Input Datset Format
----
Data records are represented by histograms that have multiple bins. Each bin has a non-negetive weight and a multi-dimensional location vector. As we assume all datasets have the same histogram definition, the locations of bins are shared by all data records. All data are encoded in text file, and followings are their formats:
- Histograms: each line represents one record, `<id> <weight of bin 0> <weight of bin 1> ...`
- Bin Locations: all locations in one line, `<bin 0 dimension 0> <bin 0 dimension 1> ... <bin n dimension 0> <bin n dimension 1> ...` 

As Melody-Join exploits the projection and normal lower bounds of EMD, it requires the projection vectors in the following format:
- Projection vector: all vectors in one line, `<vector 0 dimension 0> <vector 0 dimension 1> ... <vector n dimension 0> <vector n dimension 1> ...`

All files should be accessible via HDFS paths.

How To Run EMD Join
----
The implementation uses Apache Maven for dependency management. Additionally, make sure your environment meets the following requirement
- Apache Hadoop 2.0+
- Apache Hama 0.7.0-SNAPSHOT

Then you can run `mvn clean package assembly:single` to generate the jar file.

The following dependencies should be in the `/lib` directory of the Hadoop and Hama:
- commons-math3-3.1.1
- commons-math-2.1
- commons-collections-3.2.1
- commons-configuration1.6

Before running the generated jar using Hadoop or Hama, fill the `conf.properties` file according to the provided `melody-conf.properties`.

The algorithms can be run using the command

    hadoop jar <jar file path> com.iojin.melody.Join <conf.properties>
    hama jar <jar file path> com.iojin.melody.Join <conf.properties>

The Hama 0.7.0-SNAPSHOT binary is included under the directory `hama` for convenience.

How To Run Data Generator
----
The implementation includes a data generator which extract typical content-based features from images files and convert their format to fit the requirement of the join program. The configurations are also included in the `melody-conf.properties` file. It has three running modes, local file system, HDFS file system, and MapReduce (unavailable and in progress). The command for the local and HDFS modes is 

    java -jar <jar file path> com.iojin.melody.Generate <conf.properties>

And the command for the MapReduce mode is 
    
    hadoop jar <jar file path> com.iojin.melody.Generate <conf.properties>

The Generator currently supports the following modes

- [x] local: where the image files are read from the **local file system** and processed on the **local machine**
- [x] hdfs: where the image files are read from **hdfs** and processed on the **local machine**
- [x] mr: where the image files are read from **hdfs** and processed via **MapReduce jobs**

The Generator will support the following mode shortly

- [] crawlmr: where a file listing the image http url is read from hdfs, the image files are crawled and processed via MapReduce jobs

Supported Features
----
- `acc` Auto Color Correlogram [Jing Huang, S Ravi Kumar, Mandar Mitra, Wei-Jing Zhu, and Ramin Zabih, "Image Indexing Using Color Correlograms", CVPR, 1997] (http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.83.9300&rep=rep1&type=pdf) 
- `cedd` CEDD: Color and Edge Directivity Descriptor [Savvas A. Chatzichristofis and Yiannis Boutalis, "CEDD: Color and Edge Directivity Descriptor: A Compact Descriptor for Image Indexing and Retrieval", Computer Vision Systems, 2008] (http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.231.4&rep=rep1&type=pdf)
- `ch` Simple Color Histogram [Color Histogram Wikipedia] (http://en.wikipedia.org/wiki/Color_histogram)
- `cl` Color Layout [Jens-Rainer Ohm, Leszek Cieplinski, Heon Jun Kim, Santhana Krishnamachari, B. S. Manjunath, Dean S. Messing, and Akio Yamada, "The MPEG-7 Color Descriptors"] (http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.148.7760&rep=rep1&type=pdf)
- `eh` Edge Histogram [Chee Sun Won, Dong Kwon Park, and Soo-Jun Park, "Efficient Use of MPEG-7 Edge Histogram Descriptor", ETRI Journal, 2002] (https://www.google.com/url?sa=t&rct=j&q=&esrc=s&source=web&cd=4&cad=rja&ved=0CEwQFjAD&url=http%3A%2F%2Fetrij.etri.re.kr%2FCyber%2Fservlet%2FGetFile%3Ffileid%3DSPF-1041924741673&ei=GfTJUpODOYbriAep-IC4DA&usg=AFQjCNHjw4Wc3MUGiKY2GnwQz7A3C1POeA&sig2=llHEVUEWr4BrlcQZ1gwTUg&bvm=bv.58187178,d.aGc)
- `fcth` FCTH: Fuzzy Color and Texture Histogram [Savvas A. Chatzichristofis and Yiannis S. Boutalis, "FCTH: Fuzzy Color and Texture Histogram - A Low Level Feature for Accurate Image Retrieval", WIAMIS, 2008] (http://ieeexplore.ieee.org/xpl/login.jsp?tp=&arnumber=4556917&url=http%3A%2F%2Fieeexplore.ieee.org%2Fxpls%2Fabs_all.jsp%3Farnumber%3D4556917)
- `gabor` Gabor Texture [Dengshen Zhang, Aylwin Wong, Maria Indrawan, and Guojun Lu, "Content-based Image Retrieval Using Gabor Texture Features", PAMI, 2000] (http://pdf.aminer.org/000/318/796/rotation_invariant_texture_features_using_rotated_complex_wavelet_for_content.pdf)
- `hcedd` Hashing CEDD 
- `jcd` Joining CEDD and FCTH Histogram
- `jh` Joint Histogram [Greg Pass and Ramin Zabih, "Comparing Images Using Joint Histograms", Multimedia System, 1999] (http://www.cs.cornell.edu/~rdz/papers/pz-jms99.pdf)
- `jch` Jpeg Coefficient Histogram [Junfeng He, Zhuochen Lin, Lifeng Wang, and Xiaoou Tang, "Detecting Doctored JPEG Images Via DCT Coefficient Analysis", ECCV, 2006] (http://research.microsoft.com/pubs/69411/doctorimage_eccv06.pdf)
- `ll` Luminance Layout [Luminance Histogram] (http://marswiki.jrc.ec.europa.eu/wikicap/index.php/Luminance_Histogram)
- `oh` Opponent Histogram [Koen E. A. van de Sande, Theo Gevers, and Cees G. M. Snoek, "Evaluating Color Descriptors for Object and Scene Recognition", PAMI, 2010] (http://nichol.as/papers/Sande/Evaluation%20of%20Color%20Descriptors%20for%20Object%20and%20Scene.pdf)
- `phog` PHOG: Spatial Pyramid Kernel [Ann Bosch, Andrew Zisserman, and Xavier Munoz, "Representing shape with a spatial pyramid kernel", CVPR, 2007] (http://eprints.pascal-network.org/archive/00003009/01/bosch07.pdf)
- `sc` Scalable Color [Jens-Rainer Ohm, Leszek Cieplinski, Heon Jun Kim, Santhana Krishnamachari, B. S. Manjunath, Dean S. Messing, and Akio Yamada, "The MPEG-7 Color Descriptors"] (http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.148.7760&rep=rep1&type=pdf)
- `tamura' Tamura feature [Hideyuki Tamura, Shunji Mori, and Takashi Yamawaki, "Textural Features Corresponding to Visual Perception", TSMC, 1978] (http://ieeexplore.ieee.org/xpl/login.jsp?tp=&arnumber=4309999&url=http%3A%2F%2Fieeexplore.ieee.org%2Fxpls%2Fabs_all.jsp%3Farnumber%3D4309999)


Example Datasets
----
[Corel] (http://archive.ics.uci.edu/ml/datasets/Corel+Image+Features)

[Mirflickr] (http://medialab.liacs.nl/mirflickr/mirflickr1m/)

Note that you need to convert the format to the one specified above before feeding them to the program.
