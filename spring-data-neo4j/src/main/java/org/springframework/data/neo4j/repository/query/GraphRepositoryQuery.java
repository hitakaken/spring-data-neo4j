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

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.ogm.cypher.query.RowModelQuery;
import org.neo4j.ogm.mapper.AdHocEntityMapper;
import org.neo4j.ogm.metadata.MetaData;
import org.neo4j.ogm.model.GraphModel;
import org.neo4j.ogm.model.NodeModel;
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
                // TODO: add singleton support
                return processQueryResult(concreteType, graphQueryMethod.getQuery(), params);
            }
            return session.query(concreteType, graphQueryMethod.getQuery(), params);
        } else {
            return session.queryForObject(returnType, graphQueryMethod.getQuery(), params);
        }
    }

    private Object processQueryResult(final Class<?> queryResultType, String cypher, Map<String, Object> parameters) {
        final RowModelQuery qry = new RowModelQuery(cypher, parameters);
        return this.session.doInTransaction(new GraphCallback() {
            @Override
            public Object apply(RequestHandler requestHandler, Transaction transaction, MetaData metaData) {
                try (Neo4jResponse<RowModel> response = requestHandler.execute(qry, transaction.url())) {
                    // the tactic here is to turn each row into a NodeModel and pretend our @QueryResult class is a node entity...
                    // TODO: improve to process a row model properly
                    List<NodeModel> nodes = new ArrayList<NodeModel>();
                    for (RowModel rowModel = response.next() ; rowModel != null ; rowModel = response.next()) {
                        Map<String, Object> resultSet = new HashMap<>();
                        for (int i = 0; i < rowModel.getValues().length; i++) {
                            resultSet.put(response.columns()[i], rowModel.getValues()[i]);
                        }

                        NodeModel node = new NodeModel();
                        node.setProperties(resultSet);
                        nodes.add(node);
                    }
                    GraphModel graphModel = new GraphModel();
                    graphModel.setNodes(nodes.toArray(new NodeModel[nodes.size()]));

                    return new AdHocEntityMapper(metaData).map(queryResultType, graphModel);
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
