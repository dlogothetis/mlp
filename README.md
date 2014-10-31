This MLP implementation makes use of maven. To build the code, use:

    mvn package

To run the MLP phase, use:

    java -cp target/mlp-1.0-SNAPSHOT-jar-with-dependencies.jar MtMLP -t <number of threads> -f <input file> -b <beta parameter> -g <gamma parameter> -r <number of coarsening rounds> -k <number of partitions> -p <partitioning output file> -m <metis output file> -M <mapping file for projection> -o <graph output file>

For large graphs, it may be necessary to specify a larger heap.

After the MLP phase has completed, run metis on the file indicated previously through the -m parameter.
