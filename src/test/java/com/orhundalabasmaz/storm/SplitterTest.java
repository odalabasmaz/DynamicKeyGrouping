package com.orhundalabasmaz.storm;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * @author Orhun Dalabasmaz
 */
public class SplitterTest {

	@Test
	public void regex() {
		System.out.println(split("TESTÜTEST"));
		System.out.println(split("I've already.Checked 4u today! come ğü3.."));
		System.out.println(split("yahu arkadaş bu #trump reyiz neymiş beya @yalele"));
	}

	private List<String> split(String text) {
		return Arrays.asList(text
				.replaceAll("[^\\p{L}\\p{Nd}]+", " ")
				.split(" "));
	}
}
