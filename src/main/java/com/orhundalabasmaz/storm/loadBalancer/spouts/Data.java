package com.orhundalabasmaz.storm.loadBalancer.spouts;

import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Orhun Dalabasmaz
 */
public class Data {

	private static final List<Values> valuesList = new ArrayList<>();

	static {
		valuesList.add(new Values("argentina"));
		valuesList.add(new Values("brazil"));
		valuesList.add(new Values("canada"));
		valuesList.add(new Values("denmark"));
		valuesList.add(new Values("england"));
		valuesList.add(new Values("france"));
		valuesList.add(new Values("germany"));
		valuesList.add(new Values("holland"));
		valuesList.add(new Values("iceland"));
		valuesList.add(new Values("italy"));
		valuesList.add(new Values("jamaica"));
		valuesList.add(new Values("kenya"));
		valuesList.add(new Values("lithuania"));
		valuesList.add(new Values("malaysia"));
		valuesList.add(new Values("nigeria"));
		valuesList.add(new Values("norway"));
		valuesList.add(new Values("oman"));
		valuesList.add(new Values("peru"));
		valuesList.add(new Values("poland"));
		valuesList.add(new Values("portugal"));
		valuesList.add(new Values("qatar"));
		valuesList.add(new Values("romania"));
		valuesList.add(new Values("russia"));
		valuesList.add(new Values("spain"));
		valuesList.add(new Values("turkey"));
		valuesList.add(new Values("united states"));
		valuesList.add(new Values("venezuela"));
		valuesList.add(new Values("vietnam"));
		valuesList.add(new Values("yemen"));
		valuesList.add(new Values("zambia"));
	}

	public static List<Values> getValuesList() {
		return valuesList;
	}
}
