

public class Edge {
	private Vertex destination;
	private long weight = 1;
	
	public Edge(Vertex destination) {
	  this(destination, 1l);
	}
	
	public Edge(Vertex destination, long weight) {
		this.destination = destination;
		this.weight = weight;
	}
	
	public long getWeight() {
		return this.weight;
	}
	
	public void setWeight(long weight) {
	  this.weight = weight;
	}
	
	public Vertex getDestination() {
		return this.destination;
	}
}
