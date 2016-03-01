package org.biocaddie.datamention.download;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

/**
 * FTP utilities to list the files in an FTP directory structure
 * Source: add source here ...
 * 
 * @author Peter Rose
 *
 */
public class FtpFileLister {

	/**
	 * Returns a list of file names by recursively scanning an ftp site starting at the passed in path location
	 * @param server
	 * @param userName
	 * @param passWord
	 * @param path
	 * @return
	 */
	public static List<String> getFileNames(String server, String userName, String passWord, String path) {	
		
		List<String> files = new ArrayList<>();
		
		FTPClient ftpClient = new FTPClient();

		try {
			ftpClient.connect(server);
			showServerReply(ftpClient);

			int replyCode = ftpClient.getReplyCode();
			if (!FTPReply.isPositiveCompletion(replyCode)) {
				System.out.println("Connect failed");
				return files;
			}

			boolean success = ftpClient.login(userName, passWord);
			showServerReply(ftpClient);

			if (!success) {
				System.out.println("Could not login to the server");
				return files;
			}

			files = listDirectory(ftpClient, path, "", 0);
		
			ftpClient.disconnect();

		} catch (IOException ex) {
			System.out.println("Oops! Something wrong happened");
			ex.printStackTrace();
		} finally {
			// logs out and disconnects from server
			try {
				if (ftpClient.isConnected()) {
					ftpClient.logout();
					ftpClient.disconnect();
				}
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		return files;
	}
	
	/**
	 * Return a list of file name found by recursively scanning from the parent directory
	 * @param ftpClient
	 * @param parentDir
	 * @param currentDir
	 * @param level
	 * @return
	 * @throws IOException
	 */
	private static List<String> listDirectory(FTPClient ftpClient, String parentDir,
			String currentDir, int level) throws IOException {

		List<String> fileNames = new ArrayList<>();
		String dirToList = parentDir;
		if (!currentDir.equals("")) {
			dirToList += "/" + currentDir;
		}
		FTPFile[] subFiles = ftpClient.listFiles(dirToList);
		if (subFiles != null && subFiles.length > 0) {
			for (FTPFile aFile : subFiles) {
				String currentFileName = aFile.getName();
				if (currentFileName.equals(".")
						|| currentFileName.equals("..")) {
					// skip parent directory and directory itself
					continue;
				}
				for (int i = 0; i < level; i++) {
					System.out.print("\t");
				}
				if (aFile.isDirectory()) {
					System.out.println("Scanning inside [" + currentFileName + "]");
					fileNames.addAll(listDirectory(ftpClient, dirToList, currentFileName, level + 1));
				} else {
					System.out.println("File -> "  + currentFileName);
					fileNames.add(dirToList + "/" + currentFileName);
				}
			}
		} 
		else {
			System.out.println("-NoFiles");
		}
		return fileNames;
	}

	private static void showServerReply(FTPClient ftpClient) {
		String[] replies = ftpClient.getReplyStrings();
		if (replies != null && replies.length > 0) {
			for (String aReply : replies) {
				System.out.println("SERVER: " + aReply);
			}
		}
	}
}
