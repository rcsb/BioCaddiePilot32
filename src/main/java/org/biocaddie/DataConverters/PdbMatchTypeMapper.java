package org.biocaddie.DataConverters;



import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class PdbMatchTypeMapper implements Function<Row, Row> {
	private static final long serialVersionUID = 1L;

	@Override
	public Row call(Row row) throws Exception {
		return RowFactory.create(row.toSeq());
	}

}
