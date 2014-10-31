

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.Random;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class MtMLP {
	private static final String SPLITTER = "\\s+";
	private static Map<Long, Vertex> vertices = new Long2ObjectOpenHashMap<Vertex>();
	private static Map<Long, Long> partitions = new Long2LongOpenHashMap();
	private static Map<Long, List<Long>> mapping = new Long2ObjectOpenHashMap<List<Long>>();
	private static ReadWriteLock lock = new ReentrantReadWriteLock();
	private static long[] ids;
	private static int k;
	private static int beta;
	private static double threshold = 0.0f;

	private static void loadGraph(String inputFile) throws NumberFormatException, IOException {
		FileInputStream in = new FileInputStream(inputFile);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		long linesRead = 0;
		String line;
		while ((line = br.readLine()) != null) {
			if (line.startsWith("#")) {
				continue;
			}
			if (++linesRead % 100000 == 0) {
				System.out.println(vertices.size() + " vertices loaded...");
			}
			String[] tokens = line.split(SPLITTER);

			if (tokens.length < 2) {
				System.out.println("line has less than two tokens, too short!");
				System.exit(-1);
			}

			int idx = 0;
			long sourceId = Long.parseLong(tokens[idx++]);
			long weight = 1;

			Vertex source = vertices.get(sourceId);
			if (source == null) {
				source = new AtomicVertex(sourceId, weight);
				vertices.put(sourceId, source);
			} else {
				source.setWeight(weight);
			}

			while (idx < tokens.length) {
				long destinationId = Long.parseLong(tokens[idx++]);
				weight = 1;

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
	}

	private static void offloadMapping(String mappingFile, Map<Long, Long> origin2metis) throws IOException {
		PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(mappingFile)));

		for (Entry<Long, List<Long>> entry : mapping.entrySet()) {
			writer.print(origin2metis.get(entry.getKey()));
			for (Long id : entry.getValue()) {
				writer.print(" " + id);
			}
			writer.println();
		}
		writer.close();	  
	}

	private static void offloadGraph(String outputFile, String partitioningFile) throws IOException {
		PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(partitioningFile)));

		for (Vertex vertex : vertices.values()) {
			writer.println(vertex.getId() + " " + vertex.getLabel());
		}
		writer.close();

		writer = new PrintWriter(new BufferedWriter(new FileWriter(outputFile)));
		for (Vertex vertex : vertices.values()) {
			writer.print(vertex.getId() + " " + vertex.getWeight());
			for (Edge edge : vertex.getEdges()) {
				writer.print(" " + edge.getDestination().getId() + " " + edge.getWeight());
			}
			writer.println();
		}
		writer.close();
	}

	private static void migrateVertex(Vertex vertex, long label) {
		long currentLabel = vertex.getLabel();
		vertex.setLabel(label);

		Long counter = partitions.get(label);
		partitions.put(label, counter + vertex.getWeight());

		counter = partitions.get(currentLabel);
		partitions.put(currentLabel, counter - vertex.getWeight());
	}

	private static void init() {
		boolean firstInit = mapping.size() == 0;
		partitions.clear();
		ids = new long[vertices.size()];
		int idx = 0;
		for (Vertex vertex : vertices.values()) {
			partitions.put(vertex.getId(), vertex.getWeight());
			ids[idx++] = vertex.getId();
			if (firstInit) {
				List<Long> internal = new LongArrayList();
				internal.add(vertex.getId());
				mapping.put(vertex.getId(), internal);
			}
		}
	}

	// remove partitions that have disappeared (no more vertices with that label)
	private static int prunePartitions() {
		int prunedPartitions = 0;
		Iterator<Entry<Long, Long>> i = partitions.entrySet().iterator();
		while (i.hasNext()) {
			Entry<Long, Long> e = i.next();
			if (e.getValue() == 0) {
				i.remove();
				prunedPartitions++;
			}
		}    
		return prunedPartitions;
	}

	private static boolean overloadsPartition(long label, Vertex v) {
		return partitions.get(label) + v.getWeight() >= threshold;
	}

	private static void shuffleIds() {
		Random rnd = new Random();
		for (int i = ids.length - 1; i > 0; i--) {
			int index = rnd.nextInt(i + 1);
			// Simple swap
			long a = ids[index];
			ids[index] = ids[i];
			ids[i] = a;
		}
	}

	private static void doParallelIteration(final int numberOfThreads) throws InterruptedException {
		shuffleIds();
		final Queue<Long> qids = new ConcurrentLinkedQueue<Long>();
		for (long id : ids) {
			qids.add(id);
		}
		final AtomicLong migrations = new AtomicLong(0);
		Thread[] threads = new Thread[numberOfThreads];
		System.out.println("Starting " + numberOfThreads + " threads.");
		for (int i = 0; i < numberOfThreads; i++) {
			threads[i] = new Thread() {

				@Override
				public void run() {
					Long id;
					while ((id = qids.poll()) != null) {
						Vertex vertex = vertices.get(id);
						long label = computeLabel(vertex);
						if (label != vertex.getLabel()) {
							try {
								lock.writeLock().lock();
								if (!overloadsPartition(label, vertex)) {
									migrateVertex(vertex, label);
									migrations.addAndGet(1);
								}
							} finally {
								lock.writeLock().unlock();
							}	 
						}
					}
				} 
			};
			threads[i].start();
		}
		for (int i = 0; i < numberOfThreads; i++) {
			threads[i].join();
		}
		System.out.println("Performed " + migrations.get() + " migrations.");
	}

	private static long computeLabel(Vertex vertex) {
		Map<Long, Long> vertexWeights = new Long2LongOpenHashMap();
		Map<Long, Long> edgeWeights = new Long2LongOpenHashMap();
		for (Edge edge : vertex.getEdges()) {
			Vertex other = edge.getDestination();
			long otherLabel = other.getLabel();

			Long counter = vertexWeights.get(otherLabel);
			if (counter == null) {
				counter = 0l;
			}
			vertexWeights.put(otherLabel, counter + other.getWeight());

			counter = edgeWeights.get(otherLabel);
			if (counter == null) {
				counter = 0l;
			}
			edgeWeights.put(otherLabel, counter + edge.getWeight());
		}

		long bestLabel = vertex.getLabel();
		double maxValue = 0d;
		for (Entry<Long, Long> e : vertexWeights.entrySet()) {
			long label = e.getKey();
			double value = (double) edgeWeights.get(label) / e.getValue();
			if (value > maxValue || (value == maxValue && label < bestLabel)) {
				maxValue = value;
				bestLabel = label;
			}
		}
		return bestLabel;
	}

	private static Map<Long, List<Long>> updateMapping() {
		Map<Long, List<Long>> newMapping = new Long2ObjectOpenHashMap<List<Long>>();
		for (Vertex vertex: vertices.values()) {
			List<Long> destination = newMapping.get(vertex.getLabel());
			if (destination == null) {
				destination = new LongArrayList();
				newMapping.put(vertex.getLabel(), destination);
			}
			destination.addAll(mapping.get(vertex.getId()));
		}  
		return newMapping;
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		Options options = new Options();
		options.addOption("f", true, "input file");
		options.addOption("b", true, "beta parameter");
		options.addOption("g", true, "gamma parameter");
		options.addOption("k", true, "k partitions parameter");
		options.addOption("p", true, "partitioning output file");
		options.addOption("o", true, "graph output file");
		options.addOption("t", true, "number of threads");
		options.addOption("r", true, "number of coarsening rounds");
		options.addOption("m", true, "metis output file");
		options.addOption("M", true, "mapping file");

		CommandLineParser parser = new BasicParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			System.out.println("Problem parsing command");
			System.out.println(e);
			System.exit(-1);
		}

		if (cmd.hasOption("k")) {
			String opt = cmd.getOptionValue("k");
			k = Integer.parseInt(opt);
		}

		if (cmd.hasOption("b")) {
			String opt = cmd.getOptionValue("b");
			beta = Integer.parseInt(opt);
		}

		double gamma = 1.0d;
		if (cmd.hasOption("g")) {
			String opt = cmd.getOptionValue("g");
			gamma = Float.parseFloat(opt);
		}

		String inputFile = null;
		if (cmd.hasOption("f")) {
			inputFile = cmd.getOptionValue("f");
		} else {
			System.out.println("specify the input graph");
			System.exit(-1);
		}

		String outputFile = null;
		if (cmd.hasOption("o")) {
			outputFile = cmd.getOptionValue("o");
		} else {
			System.out.println("specify the output file");
			System.exit(-1);
		}

		String partitioningFile = null;
		if (cmd.hasOption("p")) {
			partitioningFile = cmd.getOptionValue("p");
		} else {
			System.out.println("specify the partitioning file");
			System.exit(-1);
		}

		String metisFile = null;
		if (cmd.hasOption("m")) {
			metisFile = cmd.getOptionValue("m");
		} else {
			System.out.println("specify the metis file");
			System.exit(-1);
		}

		String mappingFile = null;
		if (cmd.hasOption("M")) {
			mappingFile = cmd.getOptionValue("M");
		} else {
			System.out.println("specify the mapping file");
			System.exit(-1);
		}

		int numberOfThreads;
		if (cmd.hasOption("t")) {
			String opt = cmd.getOptionValue("t");
			numberOfThreads = Integer.parseInt(opt);
		} else {
			numberOfThreads = 1;
		}

		int numberOfRounds = 0;
		if (cmd.hasOption("r")) {
			String opt = cmd.getOptionValue("r");
			numberOfRounds = Integer.parseInt(opt);
		} else {
			System.out.println("specify the number of rounds");
			System.exit(-1);
		}

		SimpleDateFormat sdf = new SimpleDateFormat("d/M/yyyy HH:mm:ss");
		System.out.println(sdf.format(Calendar.getInstance().getTime()) + " Starting partitioning of " + inputFile);
		loadGraph(inputFile);
		threshold = vertices.size() / (gamma * k);
		long numberOfEdges = 0;
		for (Vertex v : vertices.values()) {
			numberOfEdges += v.getEdges().size();
		}
		System.out.println(sdf.format(Calendar.getInstance().getTime()) + " " + vertices.size() + " vertices and " + (numberOfEdges/2) + " edges loaded. Partitioning now...");
		for (int r = 0; r < numberOfRounds; r++) {
			init();
			for (int i = 0; i < beta; i++) {
				doParallelIteration(numberOfThreads);
				int prunedPartitions = prunePartitions();
				System.out.println(sdf.format(Calendar.getInstance().getTime()) + " Pruned " + prunedPartitions + " partitions, remained with " + partitions.size());
			}
			mapping = updateMapping();
			vertices = Utility.coarsen(vertices);
			System.out.println(sdf.format(Calendar.getInstance().getTime()) + " graph coarsened to " + vertices.size() + " vertices.");
		}
		System.out.println("Partitioning over, offloading results.");
		offloadGraph(outputFile, partitioningFile);
		Map<Long, Long> origin2metis = Utility.dumpGraphMetis(vertices, metisFile);
		offloadMapping(mappingFile, origin2metis);
		System.out.println(sdf.format(Calendar.getInstance().getTime()) + " Finished. Exiting...");
	}
}