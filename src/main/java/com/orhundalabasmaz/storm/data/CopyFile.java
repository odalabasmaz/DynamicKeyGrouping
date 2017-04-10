package com.orhundalabasmaz.storm.data;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author Orhun Dalabasmaz
 */
public class CopyFile {

	private CopyFile() {
	}

	public static void main(String[] args) throws IOException {
		String input = "D:\\cloud\\data\\pkg\\wiki-orj.txt";
		String output = "D:\\cloud\\data\\pkg\\wiki.txt";
		Path path = Paths.get(input);

		OutputStreamWriter outputStreamWriter =
				new OutputStreamWriter(new FileOutputStream(output), StandardCharsets.UTF_8);
		produce(path, outputStreamWriter);
	}

	private static long produce(Path path, OutputStreamWriter writer) throws IOException {
		long count = 0;
		try (BufferedReader br = Files.newBufferedReader(path, StandardCharsets.ISO_8859_1)) {
			String line;
			while ((line = br.readLine()) != null) {
				writer.write(line + "\n");
				++count;
			}
		}
		return count;
	}
}
