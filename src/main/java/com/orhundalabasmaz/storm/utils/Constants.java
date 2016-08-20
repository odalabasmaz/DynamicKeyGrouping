package com.orhundalabasmaz.storm.utils;

import java.io.File;

/**
 * @author Orhun Dalabasmaz
 */
public class Constants {
	public static String RESOURCE_PATH = "src/test/resources";
	public static String OUTPUT_PATH = "src/test/resources/output";
	public static File OUTPUT_DIR = new File(OUTPUT_PATH);

	public static String RESULT_FILE_HEADER =
			"test id,date time,grouping type,data set,stream type,process duration (ms),aggregation duration (ms),number of spouts,number of worker bolts," +
					"latency (ms),throughput (tuple),throughput ratio (tuple/sec),number of distinct keys,number of consumed keys";
	public static String FINAL_RESULT_FILE_HEADER =
			"test id,test count,grouping type,data set,stream type,process duration (ms),aggregation duration (ms),number of spouts,number of worker bolts," +
					"latency (ms),throughput (tuple),throughput ratio (tuple/sec),number of distinct keys,number of consumed keys,memory consumption ratio";

}
