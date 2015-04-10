/*
 * Copyright (c)  [2011-2015] "Pivotal Software, Inc." / "Neo Technology" / "Graph Aware Ltd."
 *
 * This product is licensed to you under the Apache License, Version 2.0 (the "License").
 * You may not use this product except in compliance with the License.
 *
 * This product may include a number of subcomponents with
 * separate copyright notices and license terms. Your use of the source
 * code for these subcomponents is subject to the terms and
 * conditions of the subcomponent's license, as noted in the LICENSE file.
 */

package org.springframework.data.neo4j.repository.query;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.ogm.annotation.Property;
import org.neo4j.ogm.cypher.query.RowModelQuery;
import org.neo4j.ogm.entityaccess.EntityFactory;
import org.neo4j.ogm.mapper.SingleUseEntityMapper;
import org.neo4j.ogm.metadata.MetaData;
import org.neo4j.ogm.session.GraphCallback;
import org.neo4j.ogm.session.Session;
import org.neo4j.ogm.session.request.RequestHandler;
import org.neo4j.ogm.session.response.Neo4jResponse;
import org.neo4j.ogm.session.result.RowModel;
import org.neo4j.ogm.session.transaction.Transaction;
import org.springframework.data.neo4j.annotation.QueryResult;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.RepositoryQuery;


/**
 * @author Mark Angrish
 */
public class GraphRepositoryQuery implements RepositoryQuery {

    private final GraphQueryMethod graphQueryMethod;

    private final Session session;

    public GraphRepositoryQuery(GraphQueryMethod graphQueryMethod, Session session) {
        this.graphQueryMethod = graphQueryMethod;
        this.session = session;
    }

    @Override
    public Object execute(Object[] parameters) {
        Class<?> returnType = graphQueryMethod.getMethod().getReturnType();
        Class<?> concreteType = resolveConcreteType(graphQueryMethod.getMethod().getReturnType(),
                graphQueryMethod.getMethod().getGenericReturnType());

        Map<String, Object> params = resolveParams(parameters);

        if (returnType.equals(Void.class)) {
            session.execute(graphQueryMethod.getQuery(), params);
            return null;
        } else if (Iterable.class.isAssignableFrom(returnType)) {
            // Special method to handle SDN Iterable<Map<String, Object>> behaviour.
            // TODO: Do we really want this method in an OGM? It's a little too low level and/or doesn't really fit.
            if (Map.class.isAssignableFrom(concreteType)) {
                return session.query(graphQueryMethod.getQuery(), params);
            }

            if (concreteType.isAnnotationPresent(QueryResult.class)) {

                if (concreteType.isInterface()) {
                   Iterable<Map<String, Object>> queryResults = session.query(graphQueryMethod.getQuery(), params);
                   java.util.List<Object> toReturn = new ArrayList<>();
                   for (Map<String, Object> map : queryResults) {
                       toReturn.add(Proxy.newProxyInstance(concreteType.getClassLoader(), new Class<?>[] {concreteType}, new QueryResultProxy(map)));
                   }
                   return toReturn;
                }
               return processQueryResult(concreteType, graphQueryMethod.getQuery(), params);
            }
            return session.query(concreteType, graphQueryMethod.getQuery(), params);
        } else {
            if (concreteType.isAnnotationPresent(QueryResult.class)) {
                if (concreteType.isInterface()) {
                   Iterator<Map<String, Object>> iterator = session.query(graphQueryMethod.getQuery(), params).iterator();
                   return iterator.hasNext()
                           ? Proxy.newProxyInstance(concreteType.getClassLoader(), new Class<?>[] { concreteType }, new QueryResultProxy(iterator.next()))
                           : null;
                }
                Collection<?> queryResult = processQueryResult(concreteType, graphQueryMethod.getQuery(), params);
                return queryResult.isEmpty() ? null : queryResult.iterator().next();
            }
            return session.queryForObject(returnType, graphQueryMethod.getQuery(), params);
        }
    }

    private <T> Collection<T> processQueryResult(final Class<T> queryResultType, String cypher, Map<String, Object> parameters) {
        final RowModelQuery qry = new RowModelQuery(cypher, parameters);
        return this.session.doInTransaction(new GraphCallback<Collection<T>>() {
            @Override
            public Collection<T> apply(RequestHandler requestHandler, Transaction transaction, MetaData metaData) {
                try (Neo4jResponse<RowModel> response = requestHandler.execute(qry, transaction.url())) {
                    Collection<T> toReturn = new ArrayList<>();

                    SingleUseEntityMapper entityMapper = new SingleUseEntityMapper(metaData, new EntityFactory(metaData));
                    for (RowModel rowModel = response.next(); rowModel != null; rowModel = response.next()) {
                        toReturn.add(entityMapper.map(queryResultType, response.columns(), rowModel));
                    }
                    return toReturn;
                }
            }
        });

    }

    private Map<String, Object> resolveParams(Object[] parameters) {
        Map<String, Object> params = new HashMap<>();
        Parameters<?, ?> methodParameters = graphQueryMethod.getParameters();

        for (int i = 0; i < parameters.length; i++) {
            Parameter parameter = methodParameters.getParameter(i);

            if (parameter.isNamedParameter()) {
                params.put(parameter.getName(), parameters[i]);
            } else {
                params.put("" + i, parameters[i]);
            }
        }
        return params;
    }

    public static Class<?> resolveConcreteType(Class<?> type, final Type genericType) {
        if (Iterable.class.isAssignableFrom(type)) {
            if (genericType instanceof ParameterizedType) {
                ParameterizedType returnType = (ParameterizedType) genericType;
                Type componentType = returnType.getActualTypeArguments()[0];

                return componentType instanceof ParameterizedType ?
                        (Class<?>) ((ParameterizedType) componentType).getRawType() :
                        (Class<?>) componentType;
            } else {
                return Object.class;
            }
        }

        return type;
    }

    @Override
    public GraphQueryMethod getQueryMethod() {
        return graphQueryMethod;
    }
}

class QueryResultProxy implements java.lang.reflect.InvocationHandler{

    private final Map<String, Object> data;

    public QueryResultProxy(Map<String, Object> map) {
        this.data = map;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
//        if (method.isBollocks()) {
//            log error
//        }
        Property annotation = method.getAnnotation(Property.class);
        String propertyKey;
        if (annotation == null) {
            propertyKey = method.getName().substring(3);
            propertyKey = propertyKey.substring(0,1).toLowerCase().concat(propertyKey.substring(1));
        }else {
            propertyKey = annotation.name();
        }

        return data.get(propertyKey);
    }

}