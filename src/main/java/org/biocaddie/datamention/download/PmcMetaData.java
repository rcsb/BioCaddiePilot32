package org.biocaddie.datamention.download;

import java.io.Serializable;
import java.sql.Date;

public class PmcFileEntry implements Serializable {
	private static final long serialVersionUID = 1L;
	private String fileName;
	private String citation;
	private String pmcId;
	private String pmId;
	private Integer publicationYear;
	private Date publicationDate;
	private Date updateDate;

	public PmcFileEntry(String fileName, String citation, String pmcId, String pmId, Integer publicationYear, Date publicationDate, Date updateDate) {
		this.fileName = fileName;
		this.citation = citation;
		this.pmcId = pmcId;
		this.pmId = pmId;
		this.publicationYear = publicationYear;
		this.publicationDate = publicationDate;
		this.updateDate = updateDate;		
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getCitation() {
		return citation;
	}

	public void setCitation(String citation) {
		this.citation = citation;
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

	public Integer getPublicationYear() {
		return publicationYear;
	}

	public void setPublicationYear(Integer publicationYear) {
		this.publicationYear = publicationYear;
	}

	public Date getPublicationDate() {
		return publicationDate;
	}

	public void setPublicationDate(Date publicationDate) {
		this.publicationDate = publicationDate;
	}

	public Date getUpdateDate() {
		return updateDate;
	}

	public void setUpdateDate(Date updateDate) {
		this.updateDate = updateDate;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("FileName: ");
		sb.append(fileName);
		sb.append(", PMC Id: ");
		sb.append(pmcId);
		sb.append(", PM Id: ");
		sb.append(pmId);
		sb.append(", PublicationYear: ");
		sb.append(publicationYear);
		sb.append(", PublicationDate: ");
		sb.append(publicationDate);
		sb.append(", UpdateDate: ");
		sb.append(updateDate);
		return sb.toString();
	}
}

