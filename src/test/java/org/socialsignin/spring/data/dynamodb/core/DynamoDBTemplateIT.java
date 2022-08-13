/**
 * Copyright © 2018 spring-data-dynamodb (https://github.com/rxcats/spring-data-dynamodb)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.socialsignin.spring.data.dynamodb.core;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.socialsignin.spring.data.dynamodb.domain.TransactionOperationEntity;
import org.socialsignin.spring.data.dynamodb.domain.sample.Installation;
import org.socialsignin.spring.data.dynamodb.domain.sample.User;
import org.socialsignin.spring.data.dynamodb.repository.config.EnableDynamoDBRepositories;
import org.socialsignin.spring.data.dynamodb.utils.DynamoDBLocalResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

/**
 * Integration test that interacts with DynamoDB Local instance.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { DynamoDBLocalResource.class, DynamoDBTemplateIT.TestAppConfig.class })
@TestPropertySource(properties = { "spring.data.dynamodb.entity2ddl.auto=create" })
public class DynamoDBTemplateIT {

    @Autowired
    private AmazonDynamoDB amazonDynamoDB;
    @Autowired
    private DynamoDBTemplate dynamoDBTemplate;

    @Configuration
    @EnableDynamoDBRepositories(basePackages = "org.socialsignin.spring.data.dynamodb.domain.sample")
    public static class TestAppConfig {
    }

    @Test
    public void testUser_CRUD() {

        // Given a entity to save.
        User user = new User();
        user.setName("John Doe");
        user.setNumberOfPlaylists(10);
        user.setId(UUID.randomUUID().toString());

        // Save it to DB.
        dynamoDBTemplate.save(user);

        // Retrieve it from DB.
        User retrievedUser = dynamoDBTemplate.load(User.class, user.getId());

        // Verify the details on the entity.
        assert retrievedUser.getName().equals(user.getName());
        assert retrievedUser.getId().equals(user.getId());
        assert retrievedUser.getNumberOfPlaylists() == user.getNumberOfPlaylists();

        // Update the entity and save.
        retrievedUser.setNumberOfPlaylists(20);
        dynamoDBTemplate.save(retrievedUser);

        retrievedUser = dynamoDBTemplate.load(User.class, user.getId());

        assert retrievedUser.getNumberOfPlaylists() == 20;

        // Delete.
        dynamoDBTemplate.delete(retrievedUser);

        // Get again.
        assert dynamoDBTemplate.load(User.class, user.getId()) == null;
    }

    private User createUser(String id) {
        User user = new User();
        user.setName(id);
        user.setId(id);
        return user;
    }

    private Installation createInstallation(String id) {
        Installation installation = new Installation();
        installation.setId(id);
        installation.setSystemId("SystemId");
        installation.setUpdatedAt(new Date());
        return installation;
    }

    @Test
    public void transactionWriteWithUpdate() {
        List<User> users = new ArrayList<>();
        users.add(createUser("User#1"));
        users.add(createUser("User#2"));
        users.add(createUser("User#3"));

        dynamoDBTemplate.transactionWrite(TransactionOperationEntity.withUpdate(users));

        assertNotNull(dynamoDBTemplate.load(User.class, "User#1"));
        assertNotNull(dynamoDBTemplate.load(User.class, "User#2"));
        assertNotNull(dynamoDBTemplate.load(User.class, "User#3"));
    }

    @Test
    public void transactionWriteWithDelete() {
        List<User> users = new ArrayList<>();
        users.add(createUser("User#1"));
        users.add(createUser("User#2"));
        users.add(createUser("User#3"));
        dynamoDBTemplate.batchSave(users);

        dynamoDBTemplate.transactionWrite(TransactionOperationEntity.withDelete(users));

        assertNull(dynamoDBTemplate.load(User.class, "User#1"));
        assertNull(dynamoDBTemplate.load(User.class, "User#2"));
        assertNull(dynamoDBTemplate.load(User.class, "User#3"));
    }

    @Test
    public void testTransactionWriteWithUpdateDelete() {
        List<User> users = new ArrayList<>();
        users.add(createUser("User#1"));
        users.add(createUser("User#2"));
        users.add(createUser("User#3"));
        dynamoDBTemplate.batchSave(users);

        List<User> updates = new ArrayList<>();
        updates.add(createUser("User#1"));
        updates.add(createUser("User#2"));

        List<User> deletes = new ArrayList<>();
        deletes.add(createUser("User#3"));

        dynamoDBTemplate.transactionWrite(new TransactionOperationEntity(updates, deletes));

        assertNotNull(dynamoDBTemplate.load(User.class, "User#1"));
        assertNotNull(dynamoDBTemplate.load(User.class, "User#2"));
        assertNull(dynamoDBTemplate.load(User.class, "User#3"));
    }

    @Test
    public void testDuplicateTransactionWrite() {
        List<User> users = new ArrayList<>();
        users.add(createUser("User#1"));
        users.add(createUser("User#2"));
        users.add(createUser("User#3"));
        dynamoDBTemplate.batchSave(users);

        List<User> updates = new ArrayList<>();
        updates.add(createUser("User#1"));
        updates.add(createUser("User#1"));

        assertThrows(AmazonServiceException.class,
            () -> dynamoDBTemplate.transactionWrite(TransactionOperationEntity.withUpdate(updates)));
    }

    @Test
    public void testTransactionLoad() {
        List<Object> users = new ArrayList<>();
        users.add(createUser("User#1"));
        dynamoDBTemplate.batchSave(users);
        dynamoDBTemplate.delete(createUser("User#2"));

        List<Object> targets = new ArrayList<>();
        targets.add(createUser("User#1"));
        targets.add(createUser("User#2"));

        List<Object> results = dynamoDBTemplate.transactionLoad(targets);

        assertEquals(2, results.size());
        assertNotNull(results.get(0));
        assertEquals(User.class, results.get(0).getClass());
        assertEquals("User#1", ((User) results.get(0)).getId());
        assertNull(results.get(1));
    }

}
