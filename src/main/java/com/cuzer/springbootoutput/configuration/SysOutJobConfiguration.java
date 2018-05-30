package com.cuzer.springbootoutput.configuration;

import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.cuzer.springbootoutput.itemwriters.SysOutItemWriter;

@Configuration
public class SysOutJobConfiguration {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	public ListItemReader<String> itemReader() {
		List<String> items = new ArrayList<>(100);

		for (int i = 1; i <= 100; i++) {
			items.add(String.valueOf(i));
		}

		return new ListItemReader<>(items);
	}

	public SysOutItemWriter itemWriter() {
		return new SysOutItemWriter();
	}

	public Step step1() {
		return stepBuilderFactory.get("step1").<String, String>chunk(10).reader(itemReader()).writer(itemWriter())
				.build();
	}

	@Bean
	public Job job1() {
		return jobBuilderFactory.get("SysOutJob").start(step1()).build();
	}

}
