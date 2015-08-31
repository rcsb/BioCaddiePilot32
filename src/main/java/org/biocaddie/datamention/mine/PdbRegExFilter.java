package org.biocaddie.datamention.mine;

import java.nio.charset.Charset;

import org.apache.spark.api.java.function.Function;
import org.biocaddie.PDBTools.PdbIdFinder;

import scala.Tuple2;

public class PdbRegExFilter implements Function<Tuple2<String, byte[]>, Boolean> {
	private static final long serialVersionUID = 1L;

	@Override
	public Boolean call(Tuple2<String, byte[]> data) throws Exception {
		String document = new String(data._2,Charset.forName("UTF-8"));
		return PdbIdFinder.containsPdbId(document);
	}

}
