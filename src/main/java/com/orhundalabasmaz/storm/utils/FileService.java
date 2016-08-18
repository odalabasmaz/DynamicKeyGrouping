package com.orhundalabasmaz.storm.utils;

import java.io.File;

/**
 * @author Orhun Dalabasmaz
 */
public class FileService {
	private static String resourceDir = "src/test/resources";

	public static boolean createDirectory(String dirName) {
		File file = new File(resourceDir + "/" + dirName);
		return file.mkdir();
	}

	public static boolean deleteFile(String filePath) {
		File file = new File(resourceDir + "/" + filePath);
		return file.delete();
	}
}
