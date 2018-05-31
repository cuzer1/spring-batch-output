package com.cuzer.springbootoutput.configuration;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.PassThroughLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import com.cuzer.springbootoutput.domains.Customer;
import com.cuzer.springbootoutput.domains.CustomerLineAggregator;
import com.cuzer.springbootoutput.domains.CustomerRowMapper;

@Configuration
public class FlatFileWriterJobConfiguration {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	public DataSource dataSource;

	@Bean
	public JdbcPagingItemReader<Customer> pagingItemReader() {

		JdbcPagingItemReader<Customer> reader = new JdbcPagingItemReader<>();

		reader.setDataSource(this.dataSource);
		reader.setFetchSize(10);
		reader.setRowMapper(new CustomerRowMapper());

		MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
		queryProvider.setSelectClause("id, firstName, lastName, birthdate");
		queryProvider.setFromClause("from CUSTOMER");

		Map<String, Order> sortKeys = new HashMap<>(1);

		sortKeys.put("id", Order.ASCENDING);

		queryProvider.setSortKeys(sortKeys);

		reader.setQueryProvider(queryProvider);

		return reader;
		
	}

	@Bean
	public FlatFileItemWriter<Customer> flatFileItemWriter() throws Exception {
		FlatFileItemWriter<Customer> flatFileItemWriter = new FlatFileItemWriter<>();

//		flatFileItemWriter.setLineAggregator(new PassThroughLineAggregator<>());
		 flatFileItemWriter.setLineAggregator(new CustomerLineAggregator());
		String customerOutputPath = File.createTempFile("customerOutput", "out").getAbsolutePath();
		System.out.println("Output Path: " + customerOutputPath);
		flatFileItemWriter.setResource(new FileSystemResource(customerOutputPath));
		flatFileItemWriter.afterPropertiesSet();

		return flatFileItemWriter;
	}

	@Bean
	public Step step1() throws Exception {
		return stepBuilderFactory.get("step1").<Customer, Customer>chunk(10).reader(pagingItemReader())
				.writer(flatFileItemWriter()).build();
	}

	@Bean
	public Job job() throws Exception {
		return jobBuilderFactory.get("FlatFileJob1").start(step1()).build();
	}

}
