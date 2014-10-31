

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Scanner;

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;

public class ComputeLocalityFromMetis {

	public static final String SPLITTER = "\\s+";

	/**
	 * Run as:
	 * 
	 * java es.tid.CoarsenGraph <graph file> <partition file> <output file>
	 *  
	 * Graph file in the Metis format:
	 * <vertex weight> <neighbor> <edge weight> <neighbor> <edge weight> ...
	 *  
	 * Partition file in the Metis format:
	 * <partition>
	 * 
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String args[]) throws IOException {
		if (args.length!=2) {
			System.out.println("Usage: java es.tid.CoarsenGraph <graph file> <partition file>");
			return;
		}

		String graphFile = args[0];
		String partitionFile = args[1];

		// First we make a pass over the partition file to make a vertex->partition
		// mapping.
		Map<Long, Long> vertex2partition = new Long2LongOpenHashMap();

		Scanner fileScanner = new Scanner(new File(partitionFile));

		long vertexId = 0;

		while (fileScanner.hasNextLine()) {
			String line = fileScanner.nextLine();
			String[] tokens = line.split(SPLITTER);

			vertexId++;

			if (tokens.length!=1) {
				System.out.println("Incorrect line:"+line);
				System.exit(0);
			}

			long partitionId = Long.parseLong(tokens[0]);

			// insert to vertex->partition map
			vertex2partition.put(vertexId, partitionId);
		}
		fileScanner.close();


		// Now we read the graph file vertex-by-vertex, we see which is edges are
		// local to the same partition, and compute the aggregate weight of local
		// edges.
		long totalWeight = 0;
		long totalLocalWeight = 0;

		fileScanner = new Scanner(new File(graphFile));

		// Ignore the first line which is the header
		System.out.println("ATTENTION: Ignoring first line of file (header).");
		fileScanner.nextLine();

		vertexId = 0;

		while (fileScanner.hasNextLine()) {
			String line = fileScanner.nextLine();
			String tokens[] = line.split(SPLITTER);

			vertexId++;

			long sourcePartitionId = vertex2partition.get(vertexId);

			for (int i=1; i<tokens.length; i+=2) {
				long destinationId = Long.parseLong(tokens[i]);
				long edgeWeight = Long.parseLong(tokens[i+1]);

				if (vertex2partition.get(destinationId)==null) {
					System.out.println("ERROR: No partition for destination id "+destinationId);
				}

				long destinationPartitionId = vertex2partition.get(destinationId);

				totalWeight += edgeWeight;

				if (sourcePartitionId == destinationPartitionId) {
					totalLocalWeight += edgeWeight;
				}
			}
		}
		fileScanner.close();

		System.out.println("Total:"+totalWeight);
		System.out.println("Total local weight:"+totalLocalWeight);
		System.out.println("Locality:"+((double)totalLocalWeight)/((double)totalWeight));
	}
}
