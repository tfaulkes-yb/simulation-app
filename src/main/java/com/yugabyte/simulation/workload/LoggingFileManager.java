package com.yugabyte.simulation.workload;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LoggingFileManager {

	public static final String FILE_PATH = "/tmp";
	private Map<String, BufferedWriter> openFiles = new HashMap<String, BufferedWriter>();
	
	public void createFile(String id, String heading) {
		String filePath = FILE_PATH + "/" + id + ".csv";
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(new File(filePath)), 1024);
			writer.write(heading);
			openFiles.put(id,  writer);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public void closeFile(String id) {
		BufferedWriter writer = openFiles.get(id);
		if (writer != null) {
			try {
				writer.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	public void writeLine(String id, String line) {
		BufferedWriter writer = openFiles.get(id);
		if (writer != null) {
			try {
				writer.write(line);
				writer.flush();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
