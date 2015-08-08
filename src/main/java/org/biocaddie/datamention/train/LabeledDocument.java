package org.biocaddie.datamention.train;



import java.io.Serializable;

public class LabeledDocument extends Document implements Serializable {
	private static final long serialVersionUID = 1L;
	private double label;

	public LabeledDocument(long id, String text, double label) {
		super(id, text);
		this.label = label;
	}

	public double getLabel() { return this.label; }
	public void setLabel(double label) { this.label = label; }
}
