package com.cuzer.springbootoutput.configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.support.ClassifierCompositeItemWriter;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.oxm.xstream.XStreamMarshaller;

import com.cuzer.springbootoutput.domains.Customer;
import com.cuzer.springbootoutput.domains.CustomerClassifier;
import com.cuzer.springbootoutput.domains.CustomerLineAggregator;
import com.cuzer.springbootoutput.domains.CustomerRowMapper;

@Configuration
public class WriteMultipleDestinationJobConfig {
	
	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

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
		queryProvider.setFromClause("from customer");

		Map<String, Order> sortKeys = new HashMap<>(1);

		sortKeys.put("id", Order.ASCENDING);

		queryProvider.setSortKeys(sortKeys);

		reader.setQueryProvider(queryProvider);

		return reader;
	}

	@Bean
	public FlatFileItemWriter<Customer> jsonItemWriter() throws Exception {
		FlatFileItemWriter<Customer> itemWriter = new FlatFileItemWriter<>();

		itemWriter.setLineAggregator(new CustomerLineAggregator());
		String customerOutputPath = File.createTempFile("customerOutput", ".out").getAbsolutePath();
		System.out.println(">> JSON Output Path: " + customerOutputPath);
		itemWriter.setResource(new FileSystemResource(customerOutputPath));
		itemWriter.afterPropertiesSet();

		return itemWriter;
	}

	@Bean
	public StaxEventItemWriter<Customer> xmlItemWriter() throws Exception {

		XStreamMarshaller marshaller = new XStreamMarshaller();

		Map<String, Class> aliases = new HashMap<>();
		aliases.put("customer", Customer.class);

		marshaller.setAliases(aliases);

		StaxEventItemWriter<Customer> itemWriter = new StaxEventItemWriter<>();

		itemWriter.setRootTagName("customers");
		itemWriter.setMarshaller(marshaller);
		String customerOutputPath = File.createTempFile("customerOutput", ".xml").getAbsolutePath();
		System.out.println(">> Output Path: " + customerOutputPath);
		itemWriter.setResource(new FileSystemResource(customerOutputPath));

		itemWriter.afterPropertiesSet();

		return itemWriter;
	}

//	@Bean
//	public CompositeItemWriter<Customer> itemWriter() throws Exception {
//		List<ItemWriter<? super Customer>> writers = new ArrayList<>(2);
//
//		writers.add(xmlItemWriter());
//		writers.add(jsonItemWriter());
//
//		CompositeItemWriter<Customer> itemWriter = new CompositeItemWriter<>();
//
//		itemWriter.setDelegates(writers);
//		itemWriter.afterPropertiesSet();
//
//		return itemWriter;
//	}

	@Bean
	public ClassifierCompositeItemWriter<Customer> itemWriter() throws Exception {
		ClassifierCompositeItemWriter<Customer> itemWriter = new ClassifierCompositeItemWriter<>();

		itemWriter.setClassifier(new CustomerClassifier(xmlItemWriter(), jsonItemWriter()));

		return itemWriter;
	}

	@Bean
	public Step stepMD() throws Exception {
		return stepBuilderFactory.get("stepMD")
				.<Customer, Customer>chunk(10)
				.reader(pagingItemReader())
				.writer(itemWriter())
				.stream(xmlItemWriter())
				.stream(jsonItemWriter())
				.build();
	}

	@Bean
	public Job job() throws Exception {
		return jobBuilderFactory.get("MultDestinationjob")
				.start(stepMD())
				.build();
	}

}
