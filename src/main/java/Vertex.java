

import java.util.Collection;

public interface Vertex {

	public long getId();

	public long getLabel();

	public void setLabel(long label);

	public long getWeight();

	public void setWeight(long weight);

	public void addEdge(Edge edge);

	public Edge getEdge(Long id);

	public Collection<Edge> getEdges();
}
