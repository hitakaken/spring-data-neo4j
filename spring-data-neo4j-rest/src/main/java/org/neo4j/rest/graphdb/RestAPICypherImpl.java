/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.rest.graphdb;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.impl.lucene.AbstractIndexHits;
import org.neo4j.rest.graphdb.converter.RestEntityExtractor;
import org.neo4j.rest.graphdb.entity.RestEntity;
import org.neo4j.rest.graphdb.entity.RestNode;
import org.neo4j.rest.graphdb.entity.RestRelationship;
import org.neo4j.rest.graphdb.index.IndexInfo;
import org.neo4j.rest.graphdb.index.RestIndex;
import org.neo4j.rest.graphdb.index.RestIndexManager;
import org.neo4j.rest.graphdb.query.CypherResult;
import org.neo4j.rest.graphdb.query.CypherTransaction;
import org.neo4j.rest.graphdb.query.CypherTxResult;
import org.neo4j.rest.graphdb.query.RestQueryResult;
import org.neo4j.rest.graphdb.transaction.RemoteCypherTransaction;
import org.neo4j.rest.graphdb.traversal.RestTraverser;
import org.neo4j.rest.graphdb.util.QueryResult;
import org.neo4j.rest.graphdb.util.ResultConverter;

import javax.ws.rs.core.Response.Status;
import java.util.*;

import static org.neo4j.helpers.collection.MapUtil.map;


public class RestAPICypherImpl implements RestAPI {

    public static final String _QUERY_RETURN_NODE = " RETURN id(n) as id, labels(n) as labels, n as data";
    public static final String _QUERY_RETURN_REL = " RETURN id(r) as id, type(r) as type, r as data, id(startNode(r)) as start, id(endNode(r)) as end";
    public static String MATCH_NODE_QUERY(String name) { return  " MATCH ("+name+") WHERE id("+name+") = {id_"+name+"} "; }
    public static final String _MATCH_NODE_QUERY = " MATCH (n) WHERE id(n) = {id} ";
    public static final String GET_NODE_QUERY = _MATCH_NODE_QUERY + _QUERY_RETURN_NODE;
    public static final String _MATCH_REL_QUERY = " START r=rel({id}) ";
    public static final String GET_REL_QUERY = _MATCH_REL_QUERY + _QUERY_RETURN_REL;

    public static final String GET_REL_TYPES_QUERY = _MATCH_NODE_QUERY + " MATCH (n)-[r]-() RETURN distinct type(r) as relType";

    private String createNodeQuery(Collection<String> labels) {
        String labelString = toLabelString(labels);
        return "CREATE (n" + labelString + " {props}) " + _QUERY_RETURN_NODE;
    }

    private String mergeQuery(String labelName, String key, Collection<String> labels) {
        StringBuilder setLabels = new StringBuilder();
        if (labels!=null) {
            for (String label : labels) {
                if (label.equals(labelName)) continue;
                setLabels.append("SET n:").append(label).append(" ");
            }
        }
        return "MERGE (n:`"+labelName+"` {`"+key+"`: {value}}) ON CREATE SET n={props} "+setLabels+ _QUERY_RETURN_NODE;
    }

    private String toLabelString(Collection<String> labels) {
        if (labels==null || labels.size() == 0) return "";
        StringBuilder sb = new StringBuilder();
        for (String label : labels) {
            sb.append(":").append(label);
        }
        return sb.toString();
    }

    private final RestAPI restAPI;
    private RestRequest restRequest;

    private static final ThreadLocal<CypherTransaction> cypherTransaction = new ThreadLocal<>();

    protected RestAPICypherImpl(RestAPI restAPI) {
        this.restAPI = restAPI;
        this.restRequest = restAPI.getRestRequest();
    }

    @Override
    public RestNode getNodeById(long id, Load force) {
        if (force != Load.ForceFromServer) {
            RestNode restNode = getFromCache(id);
            if (restNode != null) return restNode;
        }
        if (force == Load.FromCache) return new RestNode(RestNode.nodeUri(this, id),this);
        Iterator<List<Object>> result = query(GET_NODE_QUERY, map("id", id)).getData().iterator();
        if (!result.hasNext()) {
            throw new NotFoundException("Node not found " + id);
        }
        List<Object> row = result.next();
        return addToCache(toNode(row));
    }

    public RestNode getFromCache(long id) {
        return restAPI.getFromCache(id);
    }

    @Override
    public RestNode getNodeById(long id) {
        return getNodeById(id, Load.FromServer);
    }

    private RestNode toNode(List<Object> row) {
        long id = ((Number) row.get(0)).longValue();
        List<String> labels = (List<String>) row.get(1);
        Map<String,Object> props = (Map<String, Object>) row.get(2);
        return RestNode.fromCypher(id, labels, props, this);
    }

    private RestRelationship toRel(List<Object> row) {
        long id = ((Number) row.get(0)).longValue();
        String type = (String)row.get(1);
        Map<String,Object> props = (Map<String, Object>) row.get(2);
        long start = ((Number) row.get(3)).longValue();
        long end = ((Number) row.get(4)).longValue();
        return RestRelationship.fromCypher(id, type, props, start,end,this);
    }

    @Override
    public RestRelationship getRelationshipById(long id) {
        Iterator<List<Object>> result = query(GET_REL_QUERY, map("id", id)).getData().iterator();
        if (!result.hasNext()) {
            throw new NotFoundException("Relationship not found " + id);
        }
        List<Object> row = result.next();
        return toRel(row);
    }


    @Override
    public RestNode createNode(Map<String, Object> props) {
        return createNode(props,Collections.<String>emptyList());
    }
    @Override
    public RestNode createNode(Map<String, Object> props, Collection<String> labels) {
        Map<?, Object> data = props == null ? Collections.emptyMap() : props;
        Iterator<List<Object>> result = query(createNodeQuery(labels), map("props", data)).getData().iterator();
        if (result.hasNext()) {
            return addToCache(toNode(result.next()));
        }
        throw new RuntimeException("Error creating node with labels: " + labels + " and props: " + props + " no data returned");
    }

    @Override
    public RestNode merge(String labelName, String key, Object value, final Map<String, Object> nodeProperties, Collection<String> labels) {
        if (labelName ==null || key == null || value==null) throw new IllegalArgumentException("Label "+ labelName +" key "+key+" and value must not be null");
        Map props = nodeProperties.containsKey(key) ? nodeProperties : MapUtil.copyAndPut(nodeProperties, key, value);
        Map<String, Object> params = map("props", props, "value", value);
        Iterator<List<Object>> result = query(mergeQuery(labelName, key, labels), params).getData().iterator();
        if (!result.hasNext())
            throw new RuntimeException("Error merging node with labels: " + labelName + " key " + key + " value " + value + " labels " + labels+ " and props: " + props + " no data returned");

        return addToCache(toNode(result.next()));
    }

    public RestNode addToCache(RestNode restNode) {
        return restAPI.addToCache(restNode);
    }

    @Override
    public RestRelationship createRelationship(Node startNode, Node endNode, RelationshipType type, Map<String, Object> props) {
        String statement = MATCH_NODE_QUERY("n") + MATCH_NODE_QUERY("m") + " CREATE (n)-[r:`"+type.name()+"`]->(m) SET r={props} " + _QUERY_RETURN_REL;
        Map<String, Object> params = map("id_n", startNode.getId(), "id_m", endNode.getId(), "props", props);
        CypherTransaction.Result result = runQuery(statement, params);
        if (!result.hasData()) throw new RuntimeException("Error creating relationship from "+startNode+" to "+endNode+" type "+type.name());
        Iterator<List<Object>> it = result.getRows().iterator();
        return toRel(it.next());
    }


    @Override
    public void removeLabel(RestNode node, String label) {
        CypherTransaction.Result result = runQuery(_MATCH_NODE_QUERY + (" REMOVE n:`" + label + "` ") + _QUERY_RETURN_NODE, map("id", node.getId()));
        if (!result.hasData()) {
            throw new RuntimeException("Error removing label "+label+" from node "+node);
        }
    }

    @Override
    public Iterable<RestNode> getNodesByLabel(String label) {
        String statement = "MATCH (n:`" + label + "`) " + _QUERY_RETURN_NODE;
        return queryForNodes(statement, null);
    }

    private Iterable<RestNode> queryForNodes(String statement, Map<String, Object> params) {
        Iterable<List<Object>> result = runQuery(statement, params).getRows();
        return new IterableWrapper<RestNode,List<Object>>(result) {
            protected RestNode underlyingObjectToObject(List<Object> row) {
                return addToCache(toNode(row));
            }
        };
    }

    @Override
    public Iterable<RestNode> getNodesByLabelAndProperty(String label, String property, Object value) {
        String statement = "MATCH (n:`" + label + "`) WHERE n.`"+property+"` = {value} " + _QUERY_RETURN_NODE;
        return queryForNodes(statement, map("value", value));
    }

    @Override
    public Iterable<RelationshipType> getRelationshipTypes(RestNode node) {
        Iterable<List<Object>> result = runQuery(GET_REL_TYPES_QUERY, map("id", node.getId())).getRows();
        return new IterableWrapper<RelationshipType, List<Object>>(result) {
            protected RelationshipType underlyingObjectToObject(List<Object> row) {
                return DynamicRelationshipType.withName(row.get(0).toString());
            }
        };
    }

    @Override
    public int getDegree(RestNode restNode, RelationshipType type, Direction direction) {
        String nodeDegreeQuery = "MATCH (n)" + relPattern(direction,type) + "() WHERE id(n) = {id} RETURN count(*) as degree";
        Iterator<List<Object>> degree = runQuery(nodeDegreeQuery, map("id", restNode.getId())).getRows().iterator();
        if (!degree.hasNext()) return 0;
        return ((Number)degree.next().get(0)).intValue();
    }

    private String relPattern(Direction direction, RelationshipType... types) {
        String typeString = toTypeString(types);
        String relPattern = "--";
        if (!typeString.isEmpty()) relPattern = "-[r "+typeString+"]-";
        if (direction == Direction.OUTGOING) {
            relPattern += ">";
        } else if (direction == Direction.INCOMING) {
            relPattern = "<" + relPattern;
        }
        return relPattern;
    }

    private String toTypeString(RelationshipType... types) {
        if (types==null || types.length == 0) return "";
        StringBuilder typeString = new StringBuilder();
        for (RelationshipType type : types) {
            if (typeString.length() > 0 ) typeString.append("|");
            typeString.append(':').append('`').append(type.name()).append("`");
        }
        return typeString.toString();
    }

    @Override
    public Iterable<Relationship> getRelationships(RestNode restNode, Direction direction, RelationshipType... types) {
        String statement = _MATCH_NODE_QUERY + " MATCH (n)"+relPattern(direction,types)+"() "+_QUERY_RETURN_REL;
        CypherTransaction.Result result = runQuery(statement, map("id", restNode.getId()));
        return new IterableWrapper<Relationship,List<Object>>(result.getRows()) {
            protected Relationship underlyingObjectToObject(List<Object> row) {
                return toRel(row);
            }
        };
    }

    @Override
    public void addLabels(RestNode node, Collection<String> labels) {
        String statement = _MATCH_NODE_QUERY + " SET n"+toLabelString(labels) + _QUERY_RETURN_NODE;
        runQuery(statement,map("id",node.getId()));
        RequestResult response = getRestRequest().with(node.getUri()).post("labels", labels);

        if (response.statusOtherThan(Status.NO_CONTENT)) {
            throw new IllegalStateException("error adding labels, received " + response);
        }
    }

    public RestRequest getRestRequest() {
        return restRequest;
    }

    @Override
    public Transaction beginTx() {
        CypherTransaction tx = cypherTransaction.get();
        if (tx != null ) {
            throw new IllegalStateException("Transaction already running "+tx);
        } else {
            cypherTransaction.set(newCypherTransaction());
            return new RemoteCypherTransaction(cypherTransaction);
        }
    }

    @Override
    public <S extends PropertyContainer> IndexHits<S> getIndex(Class<S> entityType, String indexName, String key, Object value) {
        String index = key == null ? ":`" + indexName + "`({query})" : ":`" + indexName + "`(`" + key + "`={query})";
        if (Node.class.isAssignableFrom(entityType)) {
            String statement = "start n=node"+index+ _QUERY_RETURN_NODE;
            CypherTransaction.Result result = runQuery(statement, map("query", value));
            return toIndexHits(result,true);
        }
        if (Relationship.class.isAssignableFrom(entityType)) {
            String statement = "start r=rel"+index+ _QUERY_RETURN_REL;
            CypherTransaction.Result result = runQuery(statement, map("query", value));
            return toIndexHits(result,false);
        }
        throw new IllegalStateException("Unknown index entity type "+entityType);
    }

    @Override
    public <S extends PropertyContainer> IndexHits<S> queryIndex(Class<S> entityType, String indexName, String key, Object value) {
        String index =  ":`" + indexName + "`({query})";
        if (Node.class.isAssignableFrom(entityType)) {
            String statement = "start n=node"+index+ _QUERY_RETURN_NODE;
            CypherTransaction.Result result = runQuery(statement, map("query", value));
            return toIndexHits(result,true);
        }
        if (Relationship.class.isAssignableFrom(entityType)) {
            String statement = "start r=rel"+index+ _QUERY_RETURN_REL;
            CypherTransaction.Result result = runQuery(statement, map("query", value));
            return toIndexHits(result,false);
        }
        throw new IllegalStateException("Unknown index entity type "+entityType);
    }

    private <S extends PropertyContainer> IndexHits<S> toIndexHits(CypherTransaction.Result result, final boolean isNode) {
        final int size = IteratorUtil.count(result.getRows());
        final Iterator<List<Object>> it = result.getRows().iterator();
        return new AbstractIndexHits<S>() {
            @Override
            public int size() {
                return size;
            }

            @Override
            public float currentScore() {
                return 0;
            }

            @Override
            protected S fetchNextOrNull() {
                if (!it.hasNext()) return null;
                return (S)(isNode ? addToCache(toNode(it.next())) : toRel(it.next()));
            }
        };
    }

    @Override
    public RestIndexManager index() {
        return restAPI.index();
    }


    @Override
    public void deleteEntity(RestEntity entity) {
        if (entity instanceof Node) {
            runQuery(_MATCH_NODE_QUERY + " DELETE n", map("id", entity.getId()));
        } else if (entity instanceof Relationship) {
            runQuery(_MATCH_REL_QUERY  + " DELETE r", map("id", entity.getId()));
        }
    }

    @Override
    public void setPropertyOnEntity(RestEntity entity, String key, Object value) {
        if (entity instanceof Node) {
            runQuery(_MATCH_NODE_QUERY + " SET n.`"+key+"` = {value} ", map("id", entity.getId(), "value", value));
        } else if (entity instanceof Relationship) {
            runQuery(_MATCH_REL_QUERY  + " SET r.`"+key+"` = {value} ", map("id", entity.getId(), "value", value));
        }
    }

    // TODO return entity ???
    @Override
    public void setPropertiesOnEntity(RestEntity entity, Map<String, Object> properties) {
        if (entity instanceof Node) {
            runQuery(_MATCH_NODE_QUERY + " SET n = {props} ", map("id", entity.getId(), "props", properties));
        } else if (entity instanceof Relationship) {
            runQuery(_MATCH_REL_QUERY + " SET r = {props} ", map("id", entity.getId(), "props", properties));
        }
    }

    @Override
    public void removeProperty(RestEntity entity, String key) {
        if (entity instanceof Node) {
            runQuery(_MATCH_NODE_QUERY + " REMOVE n.`"+key+"`", map("id", entity.getId()));
        } else if (entity instanceof Relationship) {
            runQuery(_MATCH_REL_QUERY  + " REMOVE r.`"+key+"`", map("id", entity.getId()));
        }
    }

    // todo handle within cypher tx
    @Override
    public RestNode getOrCreateNode(RestIndex<Node> index, String key, Object value, final Map<String, Object> properties, Collection<String> labels) {
        return restAPI.getOrCreateNode(index,key,value,properties,labels);
    }

    // todo handle within cypher tx
    @Override
    public RestRelationship getOrCreateRelationship(RestIndex<Relationship> index, String key, Object value, final RestNode start, final RestNode end, final String type, final Map<String, Object> properties) {
        return restAPI.getOrCreateRelationship(index,key,value,start,end,type,properties);
    }

    public CypherResult query(String statement, Map<String, Object> params) {
        return new CypherTxResult(runQuery(statement, params));
    }

    private CypherTransaction.Result runQuery(String statement, Map<String, Object> params) {
        if (cypherTransaction.get() == null) {
            return newCypherTransaction().commit(statement,params);
        }
        return cypherTransaction.get().send(statement,params);
    }

    private CypherTransaction newCypherTransaction() {
        return new CypherTransaction(this, CypherTransaction.ResultType.row);
    }

    public QueryResult<Map<String, Object>> query(String statement, Map<String, Object> params, ResultConverter resultConverter) {
        final CypherResult result = query(statement, params);
        if (RestResultException.isExceptionResult(result.asMap())) throw new RestResultException(result.asMap());
        return RestQueryResult.toQueryResult(result, this, resultConverter);
    }

    @Override
    public RestTraverser traverse(RestNode restNode, Map<String, Object> description) {
        return restAPI.traverse(restNode, description);
    }

    public RequestResult batch(Collection<Map<String, Object>> batchRequestData) {
        return restAPI.batch(batchRequestData);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends PropertyContainer> RestIndex<T> getIndex(String indexName) {
        return restAPI.getIndex(indexName);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void createIndex(String type, String indexName, Map<String, String> config) {
        restAPI.createIndex(type, indexName, config);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends PropertyContainer> RestIndex<T> createIndex(Class<T> type, String indexName, Map<String, String> config) {
        return restAPI.createIndex(type,indexName,config);
    }

    @Override
    public void close() {
        restAPI.close();
    }

    @Override
    public boolean isAutoIndexingEnabled(Class<? extends PropertyContainer> clazz) {
        return restAPI.isAutoIndexingEnabled(clazz);
    }

    @Override
    public void setAutoIndexingEnabled(Class<? extends PropertyContainer> clazz, boolean enabled) {
        restAPI.setAutoIndexingEnabled(clazz, enabled);
    }

    @Override
    public Set<String> getAutoIndexedProperties(Class forClass) {
        return restAPI.getAutoIndexedProperties(forClass);
    }

    @Override
    public void startAutoIndexingProperty(Class forClass, String s) {
        restAPI.startAutoIndexingProperty(forClass,s);
    }

    @Override
    public void stopAutoIndexingProperty(Class forClass, String s) {
        restAPI.stopAutoIndexingProperty(forClass, s);
    }

    @Override
    public void delete(RestIndex index) {
        restAPI.delete(index);
    }

    @Override
    public <T extends PropertyContainer> void removeFromIndex(RestIndex index, T entity, String key, Object value) {
        restAPI.removeFromIndex(index, entity, key, value);
    }

    @Override
    public <T extends PropertyContainer> void removeFromIndex(RestIndex index, T entity, String key) {
        restAPI.removeFromIndex(index,entity,key);
    }

    @Override
    public <T extends PropertyContainer> void removeFromIndex(RestIndex index, T entity) {
        restAPI.removeFromIndex(index,entity);
    }


    @Override
    public <T extends PropertyContainer> void addToIndex(T entity, RestIndex index, String key, Object value) {
        restAPI.addToIndex(entity,index,key,value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends PropertyContainer> T putIfAbsent(T entity, RestIndex index, String key, Object value) {
        return restAPI.putIfAbsent(entity, index, key, value);
    }

    @Override
    public boolean hasToUpdate(long lastUpdate) {
        return restAPI.hasToUpdate(lastUpdate);
    }
    @Override
    public IndexInfo indexInfo(final String indexType) {
        return restAPI.indexInfo(indexType);
    }


    @Override
    public Collection<String> getAllLabelNames() {
        return restAPI.getAllLabelNames();
    }

    @Override
    public Iterable<RelationshipType> getRelationshipTypes() {
        return restAPI.getRelationshipTypes();
    }

    @Override
    public TraversalDescription createTraversalDescription() {
        return restAPI.createTraversalDescription();
    }

    public String getBaseUri() {
        return restRequest.getUri();
    }

    @Override
    public RestEntityExtractor getEntityExtractor() {
        return restAPI.getEntityExtractor();
    }

    @Override
    public RestEntity createRestEntity(Map data) {
        return restAPI.createRestEntity(data);
    }
}
