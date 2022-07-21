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
package org.socialsignin.spring.data.dynamodb.domain;

import org.junit.Test;
import org.socialsignin.spring.data.dynamodb.domain.sample.User;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TransactionOperationEntityTest {

    private User createUser(String id) {
        User user = new User();
        user.setName(id);
        user.setId(id);
        return user;
    }

    @Test
    public void testBuilder() {
        TransactionOperationEntity entity = TransactionOperationEntity.builder()
                .build();

        assertNull(entity.getDeleteEntities());
        assertNull(entity.getUpdateEntities());
    }

    @Test
    public void testBuilderWithUpdate() {
        List<User> users = new ArrayList<>();
        users.add(createUser("User#1"));

        TransactionOperationEntity entity = TransactionOperationEntity.builder()
                .withUpdate(users)
                .build();

        assertNull(entity.getDeleteEntities());
        assertEquals(1, entity.getUpdateEntities().size());
    }

    @Test
    public void testBuilderWithDelete() {
        List<User> users = new ArrayList<>();
        users.add(createUser("User#1"));

        TransactionOperationEntity entity = TransactionOperationEntity.builder()
                .withDelete(users)
                .build();

        assertNull(entity.getUpdateEntities());
        assertEquals(1, entity.getDeleteEntities().size());
    }

    @Test
    public void testBuilderWithUpdateDelete() {
        List<User> updates = new ArrayList<>();
        updates.add(createUser("User#1"));

        List<User> deletes = new ArrayList<>();
        deletes.add(createUser("User#9"));

        TransactionOperationEntity entity = TransactionOperationEntity.builder()
                .withUpdate(updates)
                .withDelete(deletes)
                .build();

        assertEquals(1, entity.getDeleteEntities().size());
        assertEquals(1, entity.getDeleteEntities().size());
    }

}
