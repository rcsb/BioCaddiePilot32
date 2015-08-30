package org.biocaddie.datamention.download;



import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

import scala.Tuple2;

/**
 * This class reads PubMedCentral local copies of full text XML (.nxml) and plain text (.txt) articles in the tar ball format (.tar.gz) 
 * downloaded from http://www.ncbi.nlm.nih.gov/pmc/tools/ftp/
 * 
 * @author Peter Rose
 *
 */
public class PmcTarBallReader {
	private static final int BUFFER_SIZE = 65536;
	private String fileName;
	private int batchSize;
	private int count;
	private GZIPInputStream stream;
	private TarArchiveInputStream tar;
	private TarArchiveEntry entry = null;
	private Map<String, Date> updateMap = null;
	private Date lastUpdated = null;
	
	public PmcTarBallReader(String fileName, int batchSize) throws FileNotFoundException, IOException {
		this.fileName = fileName;
		this.batchSize = batchSize;
		this.count = 1;
		stream = new GZIPInputStream(new FileInputStream(fileName), BUFFER_SIZE);
		tar = new TarArchiveInputStream(stream);
	}

	public void setUpdateMap(Map<String, Date> updateMap) {
		this.updateMap = updateMap;
	}
	
	public void setLastUpdated(Date lastUpdated) {
		this.lastUpdated = lastUpdated;
	}
	
	public String getFileName() {
		return fileName;
	}

	public int getBatchSize() {
		return batchSize;
	}
	
	public boolean hasNext() {
		return count > 0;
	}

	public List<Tuple2<String, byte[]>> getNext() throws IOException {
		List<Tuple2<String, byte[]>> pairs = new ArrayList<>(batchSize);
	
		entry = null;

		while ((entry = tar.getNextTarEntry()) != null && pairs.size() < batchSize) {
			if (entry.isDirectory()) {
				continue;
			}

			String filename = trimFilePath(entry.getName());
			Date update = updateMap.get(filename);
			if (update != null && update.before(lastUpdated)) {
				continue;
			}
			byte[] content = new byte[(int) entry.getSize()];
			tar.read(content, 0, content.length);
			pairs.add(new Tuple2<String, byte[]>(filename, content));
		}

		if (entry == null) {
			tar.close();
			stream.close();
		}
		
		count = pairs.size();
	
		return pairs;
    }
	
	private static String trimFilePath(String fileName) {
		int begin = fileName.lastIndexOf("/");
		if (begin > 0) {
			fileName = fileName.substring(begin+1);
		}
		int index = fileName.lastIndexOf(".txt");
		if (index == -1) {
			index = fileName.lastIndexOf(".nxml");
		}
		return fileName.substring(0, index);
	}
	
	public static List<String> getTarBallFileNames(String directory) throws IOException {
		List<String> fileNames = new ArrayList<>();

		DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Paths.get(directory));
		for (Path path : directoryStream) {
			if (path.toString().endsWith(".tar.gz")) {
				fileNames.add(path.toString());
			}
		}
		Collections.sort(fileNames);
		
		return fileNames;
	}
}
