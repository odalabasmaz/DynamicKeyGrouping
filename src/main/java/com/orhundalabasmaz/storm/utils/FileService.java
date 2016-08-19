package com.orhundalabasmaz.storm.utils;

import java.io.File;

import static com.orhundalabasmaz.storm.utils.Constants.RESOURCE_PATH;

/**
 * @author Orhun Dalabasmaz
 */
public class FileService {
	private static String resourceDir = RESOURCE_PATH;

	public static boolean createDirectory(String dirName) {
		File file = new File(resourceDir + "/" + dirName);
		return file.mkdir();
	}

	public static boolean deleteFile(String filePath) {
		File file = new File(resourceDir + "/" + filePath);
		return file.delete();
	}
}
