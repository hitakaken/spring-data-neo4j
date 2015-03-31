/*
 * Copyright (c)  [2011-2015] "Neo Technology" / "Graph Aware Ltd."
 *
 * This product is licensed to you under the Apache License, Version 2.0 (the "License").
 * You may not use this product except in compliance with the License.
 *
 * This product may include a number of subcomponents with
 * separate copyright notices and licence terms.  Your use of the source
 * code for these subcomponents is subject to the terms and
 * conditions of the subcomponent's licence, as noted in the LICENSE file.
 */

package org.neo4j.ogm.mapper;

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.ogm.entityaccess.DefaultEntityAccessStrategy;
import org.neo4j.ogm.entityaccess.EntityAccess;
import org.neo4j.ogm.entityaccess.EntityAccessStrategy;
import org.neo4j.ogm.entityaccess.EntityFactory;
import org.neo4j.ogm.entityaccess.PropertyReader;
import org.neo4j.ogm.entityaccess.PropertyWriter;
import org.neo4j.ogm.metadata.MetaData;
import org.neo4j.ogm.metadata.info.ClassInfo;
import org.neo4j.ogm.model.GraphModel;
import org.neo4j.ogm.model.NodeModel;
import org.neo4j.ogm.model.Property;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple implementation of {@link GraphToEntityMapper} suitable for ad-hoc entity mappings. This doesn't interact with a
 * mapping context or mandate graph IDs on the target types and is not designed for use in the OGM session.
 *
 * @author Adam George
 */
public class AdHocEntityMapper implements GraphToEntityMapper<GraphModel> {

    private static final Logger logger = LoggerFactory.getLogger(AdHocEntityMapper.class);

    private final EntityAccessStrategy entityAccessStrategy;
    private final EntityFactory entityFactory;
    private final MetaData metadata;

    private ClassInfo classInfo;

    public AdHocEntityMapper(MetaData mappingMetaData) {
        this.metadata = mappingMetaData;
        this.entityFactory = new EntityFactory(mappingMetaData);
        this.entityAccessStrategy = new DefaultEntityAccessStrategy();
    }

    public AdHocEntityMapper(ClassInfo classInfo) {
        this.metadata = null;
        this.entityFactory = new EntityFactory(null);
        this.entityAccessStrategy = new DefaultEntityAccessStrategy();
        this.classInfo = classInfo;
    }

    @Override
    public <T> Collection<T> map(Class<T> type, GraphModel graphModel) {
        Collection<T> col = new ArrayList<>(graphModel.getNodes().length);
        for (NodeModel node : graphModel.getNodes()) {
            T entity = this.entityFactory.newObject(type);
            setProperties(node, entity);
            col.add(entity);
        }
        return col;
    }

    private void setProperties(NodeModel nodeModel, Object instance) {
        ClassInfo classInfo = this.classInfo == null ? this.metadata.classInfo(instance) : this.classInfo;
        for (Property<?, ?> property : nodeModel.getPropertyList()) {
            writeProperty(classInfo, instance, property);
        }
    }

    // TODO: the following is all pretty much copied from GraphEntityMapper so should probably be refactored
    private void writeProperty(ClassInfo classInfo, Object instance, Property<?, ?> property) {
        PropertyWriter writer = this.entityAccessStrategy.getPropertyWriter(classInfo, property.getKey().toString());

        if (writer == null) {
            logger.warn("Unable to find property: {} on class: {} for writing", property.getKey(), classInfo.name());
        } else {
            Object value = property.getValue();
            // merge iterable / arrays and co-erce to the correct attribute type
            if (writer.type().isArray() || Iterable.class.isAssignableFrom(writer.type())) {
                PropertyReader reader = this.entityAccessStrategy.getPropertyReader(classInfo, property.getKey().toString());
                if (reader != null) {
                    Object currentValue = reader.read(instance);
                    Class<?> paramType = writer.type();
                    value = paramType.isArray()
                            ? EntityAccess.merge(paramType, (Iterable<?>) value, (Object[]) currentValue)
                            : EntityAccess.merge(paramType, (Iterable<?>) value, (Iterable<?>) currentValue);
                }
            }
            writer.write(instance, value);
        }
    }

}
