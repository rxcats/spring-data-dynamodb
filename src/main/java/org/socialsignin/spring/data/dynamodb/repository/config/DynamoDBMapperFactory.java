package org.socialsignin.spring.data.dynamodb.repository.config;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Autowired;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;

public class DynamoDBMapperFactory implements FactoryBean<DynamoDBMapper> {

	private final AmazonDynamoDB amazonDynamoDB;
	private final DynamoDBMapperConfig dynamoDBMapperConfig;
	
	@Autowired
	public DynamoDBMapperFactory(AmazonDynamoDB amazonDynamoDB, DynamoDBMapperConfig dynamoDBMapperConfig) {
		this.amazonDynamoDB = amazonDynamoDB;
		this.dynamoDBMapperConfig = dynamoDBMapperConfig;
	}

	@Override
	public DynamoDBMapper getObject() throws Exception {
		return new DynamoDBMapper(amazonDynamoDB, dynamoDBMapperConfig);
	}

	@Override
	public Class<?> getObjectType() {
		return DynamoDBMapper.class;
	}

}
