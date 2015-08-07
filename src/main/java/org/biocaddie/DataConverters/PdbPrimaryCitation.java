package org.biocaddie.DataConverters;



import java.io.Serializable;
import java.sql.Date;

public class PdbPrimaryCitation implements Serializable {
	private static final long serialVersionUID = 1L;
	
	public static final Integer CURRENT = 1;
	public static final Integer OBSOLETE = 2;
	public static final Integer UNRELEASED = 3;
	public static final Integer UNASSIGNED = 4;
	public static final Integer CURRENT_MODEL = 5;
	public static final Integer OBSOLETE_MODEL = 6;
	
	private String pdbId;
	private String pmcId;
	private String pmId;
	private Integer depositionYear;
	private Date depositionDate;
	private Integer entryType;

	public PdbPrimaryCitation(){};
	
	public PdbPrimaryCitation(String pdbId, String pmcId, String pmId,
			Integer depositionYear, Date depositionDate, Integer entryType) {
		this.pdbId = pdbId;
		this.pmcId = pmcId;
		this.pmId = pmId;
		this.depositionYear = depositionYear;
		this.depositionDate = depositionDate;
		this.entryType = entryType;
	}

	public String getPdbId() {
		return pdbId;
	}
	public void setPdbId(String pdbId) {
		this.pdbId = pdbId;
	}
	public String getPmcId() {
		return pmcId;
	}
	public void setPmcId(String pmcId) {
		this.pmcId = pmcId;
	}
	public String getPmId() {
		return pmId;
	}
	public void setPmId(String pmId) {
		this.pmId = pmId;
	}

	public Integer getDepositionYear() {
		return depositionYear;
	}

	public void setDepositionYear(Integer depositionYear) {
		this.depositionYear = depositionYear;
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
}
