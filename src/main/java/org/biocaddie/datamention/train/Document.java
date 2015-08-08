package org.biocaddie.datamention.train;



import java.io.Serializable;

//Labeled and unlabeled instance types.
//Spark SQL can infer schema from Java Beans.
public class Document implements Serializable {
	private static final long serialVersionUID = 1L;
	private long id;
	private String text;

	public Document(long id, String text) {
		this.id = id;
		this.text = text;
	}

	public long getId() { return this.id; }
	public void setId(long id) { this.id = id; }

	public String getText() { return this.text; }
	public void setText(String text) { this.text = text; }
}
