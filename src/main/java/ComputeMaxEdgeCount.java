

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;

public class ComputeMaxEdgeCount {

	public static final String SPLITTER = "\\s+";

	/**
	 * 
	 * Takes as input the graph and the partition membership (every partition with
	 * the vertices it contains). It finds the partition with the largest total
	 * number of edges contained in it, and prints the maximum total number of 
	 * edges.
	 * 
	 * 
	 * Run as:
	 * 
	 * java es.tid.ComputeMaxEdgeCount <graph file> <partition membership file>
	 *  
	 * Graph file format:
	 * <vertex id> <vertex weight> <neighbor> <edge weight> <neighbor> <edge weight> ...
	 *  
	 * Partition membership file format:
	 * <partition id> <vertex id> <vertex id> <vertex id> ...
	 * 
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String args[]) throws IOException {
		if (args.length != 3) {
			System.out.println("Usage: java es.tid.CoarsenGraph <graph file> <partition file> <metis output file>");
			return;
		}

		Map<Long, Long> counters = new Long2LongOpenHashMap();
		Map<Long, Long> metisOutput = new Long2LongOpenHashMap();

		String graphFile = args[0];
		String partitionFile = args[1];
		String metisOutputFile = args[2];

		Map<Long, Vertex> graph = Utility.loadGraph(graphFile, true);

		Long metisID = 1l;
		Scanner fileScanner = new Scanner(new File(metisOutputFile));    
		while (fileScanner.hasNextLine()) {
			String line = fileScanner.nextLine();
			Long partitionID = Long.valueOf(line);
			metisOutput.put(metisID++, partitionID);
			if (!counters.containsKey(partitionID)) {
				counters.put(partitionID, 0l);
			}
		}
		fileScanner.close();

		fileScanner = new Scanner(new File(partitionFile));
		while (fileScanner.hasNextLine()) {
			String line = fileScanner.nextLine();
			String[] tokens = line.split(SPLITTER);
			metisID = Long.valueOf(tokens[0]);
			long counter = 0;
			for (int i = 1; i < tokens.length; i++) {
				Long vid = Long.valueOf(tokens[i]);
				counter += graph.get(vid).getEdges().size();
			}
			Long partitionID = metisOutput.get(metisID);
			Long tmp = counters.get(partitionID);
			counters.put(partitionID, tmp + counter);    	
		}
		fileScanner.close();

		long total = 0;
		long maxValue = 0;
		for (Entry<Long, Long> e : counters.entrySet()) {
			total += e.getValue();
			maxValue = Math.max(maxValue, e.getValue());
		}
		// maximum normalized load
		double mnl = ((double) maxValue) / (((double) total) / counters.size());
		System.out.println("total edges=" + total + " max loaded partition=" + maxValue + " maximum normalized load=" + mnl);    
	}
}
