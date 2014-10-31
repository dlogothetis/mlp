

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.Collection;
import java.util.Map;

public class AtomicVertex implements Vertex {
	private Map<Long, Edge> edges = new Long2ObjectOpenHashMap<Edge>();
	private long id;
	private volatile long label;
	private long weight;

	public AtomicVertex(long id) {
		this(id, 1l);
	}

	public AtomicVertex(long id, long weight) {
		this.id = id;
		this.label = id;
		this.weight = weight;
	}

	public long getId() {
		return this.id;
	}

	public long getLabel() {
		return this.label;
	}

	public void setLabel(long label) {
		this.label = label;
	}

	public long getWeight() {
		return this.weight;
	}

	public void setWeight(long weight) {
		this.weight = weight;
	}

	public void addEdge(Edge edge) {
		long otherId = edge.getDestination().getId();
		edges.put(otherId, edge);
	}

	public Edge getEdge(Long id) {
		return edges.get(id);
	}

	public Collection<Edge> getEdges() {
		return this.edges.values();
	}

	public int hashCode() {
		return (int) this.id;
	}

	public boolean equals(Object o) {
		if (!(o instanceof AtomicVertex)) {
			return false;
		}
		AtomicVertex other = (AtomicVertex) o;
		return this.id == other.id;
	}
}
