package com.cuzer.springbootoutput.domains;

import org.springframework.batch.item.file.transform.LineAggregator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CustomerLineAggregator implements LineAggregator<Customer> {
	
	private ObjectMapper objectMapper = new ObjectMapper();

	@Override
	public String aggregate(Customer item) {
		
		try {
			return objectMapper.writeValueAsString(item);
		} catch (JsonProcessingException e) {
			throw new RuntimeException("Unable to serialize Customer", e);
		}
		
	}

}
