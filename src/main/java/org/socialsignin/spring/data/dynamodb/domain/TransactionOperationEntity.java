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

import java.util.List;

public class TransactionOperationEntity {
    private final List<?> updateEntities;
    private final List<?> deleteEntities;

    public TransactionOperationEntity(List<?> updateEntities, List<?> deleteEntities) {
        this.updateEntities = updateEntities;
        this.deleteEntities = deleteEntities;
    }

    public static TransactionOperationEntity withUpdate(List<?> entities) {
        return new TransactionOperationEntity(entities, null);
    }

    public static TransactionOperationEntity withDelete(List<?> entities) {
        return new TransactionOperationEntity(null, entities);
    }

    public List<?> getUpdateEntities() {
        return updateEntities;
    }

    public List<?> getDeleteEntities() {
        return deleteEntities;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private List<?> updateEntities;
        private List<?> deleteEntities;

        private Builder() {

        }

        public Builder withUpdate(List<?> entities) {
            this.updateEntities = entities;
            return this;
        }

        public Builder withDelete(List<?> entities) {
            this.deleteEntities = entities;
            return this;
        }

        public TransactionOperationEntity build() {
            return new TransactionOperationEntity(updateEntities, deleteEntities);
        }

    }
}
