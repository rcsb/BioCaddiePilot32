package org.biocaddie.PDBTools;





import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.SortedSet;
import java.util.TreeSet;

public class PdbIdSource {
	private static String allUrl = "http://www.rcsb.org/pdb/rest/getCurrent/";
	/**
	 * Returns the current list of all PDB IDs.
	 * @return PdbChainKey set of all PDB IDs.
	 */
	public static SortedSet<String> getAll() {
		SortedSet<String> representatives = new TreeSet<String>();

		try {
			URL u = new URL(allUrl);
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(u.openStream()));

			String line = null;

			while ((line = reader.readLine()) != null) {
				int index = line.lastIndexOf("structureId=");
				if (index > 0) {
					representatives.add(line.substring(index + 13, index + 17));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("PDB IDs: " + representatives.size());

		return representatives;
	}
}
