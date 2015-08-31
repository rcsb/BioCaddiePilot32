package org.biocaddie.datamention.train;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class ObjectSerializer {	 

	public static void serialize(Object obj, String fileName) throws IOException {
		FileOutputStream os = new FileOutputStream(fileName);
		ObjectOutputStream oos = new ObjectOutputStream(os);
		oos.writeObject(obj);
		os.close();
	}

	public static Object deserialize(String fileName) throws IOException, ClassNotFoundException {
		FileInputStream is = new FileInputStream(fileName);
		ObjectInputStream os = new ObjectInputStream(is);
		Object obj = os.readObject();
		os.close();
		return obj;
	}
}
