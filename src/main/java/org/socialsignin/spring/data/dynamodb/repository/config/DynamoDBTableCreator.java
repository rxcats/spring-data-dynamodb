package org.socialsignin.spring.data.dynamodb.repository.config;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableResult;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.TableUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.socialsignin.spring.data.dynamodb.repository.DynamoDBCrudRepository;
import org.socialsignin.spring.data.dynamodb.repository.DynamoDBPagingAndSortingRepository;
import org.socialsignin.spring.data.dynamodb.repository.support.DynamoDBEntityInformation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.repository.Repository;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

@Component
public class DynamoDBTableCreator implements ApplicationListener<ContextRefreshedEvent> {
    private final static Logger LOGGER = LoggerFactory.getLogger(DynamoDBTableCreator.class);
    public static final String CONFIGURATION_KEY = "spring.data.dynamodb.entity2ddl.auto";

    private final AmazonDynamoDB amazonDynamoDB;
    private final DynamoDBMapper mapper;

    private final ProvisionedThroughput pt;
    private final ProjectionType gsiProjectionType;

    /**
     * Configuration key is  {@code spring.data.dynamodb.entity2ddl.auto}
     * Inspired by Hibernate's hbm2ddl
     * @see <a href="https://docs.jboss.org/hibernate/orm/5.2/userguide/html_single/Hibernate_User_Guide.html#configurations-hbmddl">Hibernate User Guide</a>
     */
    public enum Entity2DDL {
        /** No action will be performed. */
        NONE("none"),

        /** Database creation will be generated. */
        CREATE_ONLY("create-only"),

        /** Database dropping will be generated. */
        DROP("drop"),

        /** Database dropping will be generated followed by database creation. */
        CREATE("create"),

        /** Drop the schema and recreate it on SessionFactory startup. Additionally, drop the schema on ApplicationContext shutdown. */
        CREATE_DROP("create-drop"),

        /** Validate the database schema */
        VALIDATE("validate");

        private final String configurationValue;

        Entity2DDL(String configurationValue) {
            this.configurationValue = configurationValue;
        }

        public String getConfigurationValue() {
            return this.configurationValue;
        }

        public static Entity2DDL fromConfigurationValue(String value) {
            for (Entity2DDL resolvedConfig : Entity2DDL.values()) {
                if (resolvedConfig.configurationValue.equals(value)) {
                    return resolvedConfig;
                }
            }
            throw new IllegalArgumentException(value + " is not a valid configuration value!");
        }
    }

    @Autowired
    public DynamoDBTableCreator(AmazonDynamoDB amazonDynamoDB, /*DynamoDBMapper mapper,*/ Optional<TaskScheduler> taskScheduler) {
        this(amazonDynamoDB, null, 10L, 10L, ProjectionType.ALL, taskScheduler);
    }

    public DynamoDBTableCreator(AmazonDynamoDB amazonDynamoDB, DynamoDBMapper mapper,
                                long readCapacity, long writeCapacity, ProjectionType gsiProjectionType, Optional<TaskScheduler> taskScheduler) {
        this.amazonDynamoDB = amazonDynamoDB;
        this.mapper = mapper;

        this.pt = new ProvisionedThroughput(readCapacity, writeCapacity);
        this.gsiProjectionType = gsiProjectionType;
    }

    protected <T, ID>  Runnable generateRunnable(Entity2DDL operation, DynamoDBEntityInformation<T, ID> entityInformation) {
    	return () -> {
			execute(operation, entityInformation);
		};
    }
    
    public <T, ID> void execute(Entity2DDL operation, DynamoDBEntityInformation<T, ID> entityInformation) {
        switch (operation) {
            case CREATE:
            case CREATE_DROP:
                drop(entityInformation);
            case CREATE_ONLY:
                create(entityInformation);
                break;
            case VALIDATE:
                validate(entityInformation);
                break;
            case DROP:
                drop(entityInformation);
            case NONE:
            default:
                LOGGER.debug("No auto table DDL performed");
                break;
        }
    }

    protected <T, ID> CreateTableResult create(DynamoDBEntityInformation<T, ID> entityInformation) {
    	Class<T> domainType = entityInformation.getJavaType();
    	LOGGER.trace("Creating table for entity {}", domainType);

        CreateTableRequest ctr = mapper.generateCreateTableRequest(domainType);
        ctr.setProvisionedThroughput(pt);

        if (ctr.getGlobalSecondaryIndexes() != null) {
            ctr.getGlobalSecondaryIndexes().forEach(gsi -> {
                gsi.setProjection(new Projection().withProjectionType(gsiProjectionType));
                gsi.setProvisionedThroughput(pt);
            });
        }

        CreateTableResult ctResponse = amazonDynamoDB.createTable(ctr);

        TableUtils.createTableIfNotExists(amazonDynamoDB, ctr);
        LOGGER.debug("Created table {} for entity {}", ctr.getTableName(), domainType);

        return ctResponse;
    }

    protected <T, ID> DeleteTableResult drop(DynamoDBEntityInformation<T, ID> entityInformation) {
        Class<T> domainType = entityInformation.getJavaType();
        LOGGER.trace("Creating table for entity {}", domainType);

        
        DeleteTableRequest dtr = mapper.generateDeleteTableRequest(domainType);
        DeleteTableResult dtResponse = amazonDynamoDB.deleteTable(dtr);

        TableUtils.deleteTableIfExists(amazonDynamoDB, dtr);
        LOGGER.debug("Deleted table {} for entity {}", dtr.getTableName(), domainType);

        return dtResponse;
    }

    /**
     * @param entityInformation The entity to check for it's table
     * @throws IllegalStateException is thrown if the existing table doesn't match the entity's annotation
     */
    protected <T, ID> void validate(DynamoDBEntityInformation<T, ID> entityInformation) throws IllegalStateException {
        Class<T> domainType = entityInformation.getJavaType();

        CreateTableRequest expected = mapper.generateCreateTableRequest(domainType);

        TableDescription actual = amazonDynamoDB.describeTable(expected.getTableName()).getTable();

        if (!expected.getKeySchema().equals(actual.getKeySchema())) {
            throw new IllegalStateException("KeySchema is not as expected. Expected: <" + expected.getKeySchema()
                    + "> but found <" + actual.getKeySchema() + ">");
        }
        LOGGER.debug("KeySchema is valid");


        if (expected.getGlobalSecondaryIndexes() != null) {
            if (!expected.getGlobalSecondaryIndexes().equals(actual.getGlobalSecondaryIndexes())) {
                throw new IllegalStateException("Global Secondary Indexes are not as expected. Expected: <" + expected.getGlobalSecondaryIndexes()
                        + "> but found <" + actual.getGlobalSecondaryIndexes() + ">");
            }
        }
        LOGGER.debug("Global Secondary Indexes are valid");
    }

	@Override
	public void onApplicationEvent(ContextRefreshedEvent event) {
		ApplicationContext appContext = event.getApplicationContext();

		Map<String, Repository> repositories = new HashMap<>();
		repositories.putAll(appContext.getBeansOfType(DynamoDBCrudRepository.class));
		repositories.putAll(appContext.getBeansOfType(DynamoDBPagingAndSortingRepository.class));
		
		LOGGER.info("Checking repositories {}", repositories.keySet());
	}

}
