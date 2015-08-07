package org.biocaddie.DataConverters;



import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class PdbEntryStatus implements Serializable {
	private static final long serialVersionUID = 1L;
	public static final Integer CURRENT = 1;
	public static final Integer OBSOLETE = 2;
	public static final Integer UNRELEASED = 3;
	public static final Integer UNASSIGNED = 4;
	public static final Integer CURRENT_MODEL = 5;
	public static final Integer OBSOLETE_MODEL = 6;
	
	private String pdbId;
    private Date depositionDate;
    private Integer entryType = UNASSIGNED;
    private String pmId;
    private String pmcId;
    private Integer publicationYear;
    
	private DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    
	public String getPdbId() {
		return pdbId;
	}
	public void setPdbId(String pdbId) {
		this.pdbId = pdbId;
	}
	public Date getDepositionDate() {
		return depositionDate;
	}
	public void setDepositionDate(Date depositionDate) {
		this.depositionDate = depositionDate;
	}
	public Integer getEntryType() {
		return entryType;
	}
	public void setEntryType(Integer entryType) {
		this.entryType = entryType;
	}
	public String getPmId() {
		return pmId;
	}
	public void setPmId(String pmId) {
		this.pmId = pmId;
	}
	public String getPmcId() {
		return pmcId;
	}
	public void setPmcId(String pmcId) {
		this.pmcId = pmcId;
	}
	public Integer getPublicationYear() {
		return publicationYear;
	}
	public void setPublicationYear(Integer publicationYear) {
		this.publicationYear = publicationYear;
	}
	
	public String toString() {
		return toCsv();
	}
	
	public String toCsv() {
		StringBuilder sb = new StringBuilder();
		sb.append(pdbId);
		sb.append(",");
		if (depositionDate != null) {
			sb.append(format.format(depositionDate));
		} else {
			sb.append("null");
		}
		sb.append(",");
		sb.append(entryType);
		sb.append(",");
		sb.append(pmId);
		sb.append(",");
		sb.append(pmcId);
		sb.append(",");
		sb.append(publicationYear);
		return sb.toString();
	}
}
