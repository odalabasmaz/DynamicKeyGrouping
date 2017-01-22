package com.orhundalabasmaz.storm.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

/**
 * @author Orhun Dalabasmaz
 */
public class JsonEncoder implements Encoder<Object> {
	private ObjectMapper mapper = new ObjectMapper();

	public JsonEncoder(VerifiableProperties verifiableProperties) {
		/* This constructor must be present for successful compile. */
	}

	@Override
	public byte[] toBytes(Object object) {
		try {
			return mapper.writeValueAsBytes(object);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
}
