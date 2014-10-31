

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Map;

public class Utility {
	private static final String SPLITTER = "\\s+";

	/**
	 * Dumps the graph in simple format
	 * @param graph
	 * @param file
	 * @throws IOException
	 */
	public static void dumpGraph(Map<Long, Vertex> graph, String file) throws IOException {
		PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(file)));

		for (Vertex v : graph.values()) {
			writer.print(v.getId()+" "+v.getWeight());
			for (Edge e : v.getEdges()) {
				writer.print(" "+e.getDestination().getId()+" "+e.getWeight());
			}
			writer.println();
		}
		writer.close();
	}

	/**
	 * Dumps the graph in the metis format.
	 * @param graph
	 * @param file
	 * @throws IOException
	 */
	public static Map<Long, Long> dumpGraphMetis(Map<Long, Vertex> graph, String file) throws IOException {
		PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(file)));

		// Re-index vertices for the METIS format. The i-th element of 
		// the metis2origin array is the id of the original graph.
		ArrayList<Long> metis2origin = new ArrayList<Long>();

		Map<Long, Long> origin2metis = new Long2LongOpenHashMap();

		long edgeCount = 0;

		metis2origin.add(null); //0-th element
		long index = 0;
		for (Vertex v : graph.values()) {
			index++;
			metis2origin.add(v.getId());
			origin2metis.put(v.getId(), index);
			edgeCount += v.getEdges().size();
		}

		writer.println(metis2origin.size()-1+" "+edgeCount/2+" 011 1");
		for (int i=1; i<metis2origin.size(); i++) {
			Vertex original = graph.get(metis2origin.get(i));
			writer.print(original.getWeight());
			for (Edge e : original.getEdges()) {
				writer.print(" "+origin2metis.get(e.getDestination().getId())+" "+e.getWeight());
			}
			writer.println();
		}
		writer.close();

		return origin2metis;
	}

	public static Map<Long, Vertex> coarsen(Map<Long, Vertex> vertices) {
		Map<Long, Vertex> coarseGraph = new Long2ObjectOpenHashMap<Vertex>();

		for (Vertex v : vertices.values()) {
			long partitionId = v.getLabel();
			Vertex coarseVertex = coarseGraph.get(new Long(partitionId));
			if (coarseVertex==null) {
				coarseVertex = new AtomicVertex(partitionId, v.getWeight());
				coarseGraph.put(partitionId, coarseVertex);
			} else {
				coarseVertex.setWeight(coarseVertex.getWeight()+v.getWeight());
			}

			for (Edge e : v.getEdges()) {
				long destinationPartitionId = e.getDestination().getLabel();
				if (partitionId!=destinationPartitionId) {
					Vertex coarseDestinationVertex = coarseGraph.get(destinationPartitionId);
					if (coarseDestinationVertex == null) {
						// Edges refer to the same vertex object, and it's possible that the 
						// destination coarse vertex object doesn't already exist, so we create it.
						coarseDestinationVertex = new AtomicVertex(destinationPartitionId, 0);
						coarseGraph.put(coarseDestinationVertex.getId(), coarseDestinationVertex);
					} 
					Edge coarseEdge = coarseVertex.getEdge(coarseDestinationVertex.getId());
					if (coarseEdge==null) {
						coarseVertex.addEdge(new Edge(coarseDestinationVertex, e.getWeight()));
					} else {
						coarseEdge.setWeight(coarseEdge.getWeight()+e.getWeight());
					}
				}
			}
		}
		return coarseGraph;	  
	}

	/**
	 * Coarsens the graph given a partitioning.
	 * 
	 * @param vertices
	 * @param vertex2partition
	 * @return
	 */
	public static Map<Long, Vertex> coarsen(Map<Long, Vertex> vertices, 
			Map<Long, Long> vertex2partition) {

		for (Vertex v : vertices.values()) {
			v.setLabel(vertex2partition.get(v.getId()));
		}
		return coarsen(vertices);
	}

	public static Map<Long, Vertex> loadGraph(String inputFile, boolean initialPartitioning) 
			throws NumberFormatException, IOException {
		FileInputStream in = new FileInputStream(inputFile);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		Map<Long, Vertex> vertices = new Long2ObjectOpenHashMap<Vertex>();

		String line;
		while ((line = br.readLine()) != null) {
			if (line.startsWith("#")) {
				continue;
			}
			String[] tokens = line.split(SPLITTER);

			if (tokens.length < 2) {
				System.out.println("line has less than two tokens, too short!");
				System.exit(-1);
			}

			int idx = 0;
			long sourceId = Long.parseLong(tokens[idx++]);
			long weight;
			if (initialPartitioning) {
				weight = 1;
			} else {
				weight = Long.parseLong(tokens[idx++]);
			}

			Vertex source = vertices.get(sourceId);
			if (source == null) {
				source = new AtomicVertex(sourceId, weight);
				vertices.put(sourceId, source);
			} else {
				source.setWeight(weight);
			}

			while (idx < tokens.length) {
				long destinationId = Long.parseLong(tokens[idx++]);
				if (initialPartitioning) {
					weight = 1;
				} else {
					weight = Long.parseLong(tokens[idx++]);
				}

				Vertex destination = vertices.get(destinationId);
				if (destination == null) {
					destination = new AtomicVertex(destinationId);
					vertices.put(destinationId, destination);
				}
				source.addEdge(new Edge(destination, weight));
				destination.addEdge(new Edge(source, weight));
			}
		}
		br.close();

		return vertices;
	}
}
