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

	@Test
	public void langSplitter() {
		assert "aa".equals("aa".split("\\.")[0]);
		assert "ab".equals("ab.zero.d".split("\\.")[0]);
		assert "aa".equals("aa.zero".split("\\.")[0]);
	}

	@Test
	public void format() {
//		double d = 5.28117597461604;
		double d = 12314.94140007597461604;
		String df = String.format("%.0f", d);
		System.out.println(df);
	}
}
