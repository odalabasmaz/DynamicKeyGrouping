package com.orhundalabasmaz.storm.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

import java.io.IOException;

/**
 * @author Orhun Dalabasmaz
 */
public class JsonDecoder implements Decoder<Object> {
	private ObjectMapper objectMapper = new ObjectMapper();

	public JsonDecoder(VerifiableProperties verifiableProperties) {
		/* This constructor must be present for successful compile. */
	}

	@Override
	public Object fromBytes(byte[] bytes) {
		if (bytes == null) {
			return null;
		}

		try {
			return objectMapper.readValue(bytes, Object.class);
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
}
