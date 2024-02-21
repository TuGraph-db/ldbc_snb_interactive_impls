package ai.fma.impls.workloads.ldbc.snb.lightgraph.interactive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashSet;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;
import org.json.JSONArray;

import com.google.common.collect.Lists;
import org.ldbcouncil.snb.driver.Db;
import org.ldbcouncil.snb.driver.DbConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.control.LoggingService;
import org.ldbcouncil.snb.driver.workloads.interactive.*;

import ai.fma.impls.workloads.ldbc.snb.lightgraph.LightGraphCypherDbConnectionState;
import ai.fma.impls.workloads.ldbc.snb.lightgraph.CustomDataInputStream;
import ai.fma.impls.workloads.ldbc.snb.lightgraph.CustomDataOutputStream;
import ai.fma.impls.workloads.ldbc.snb.lightgraph.LightGraphClient;
import lgraph.Lgraph;

public class LightGraphInteractiveCypherRpcDb extends Db {

    LightGraphCypherDbConnectionState dcs;

    @Override
    protected void onInit(Map<String, String> properties, LoggingService loggingService) throws DbException {
        try {
            dcs = new LightGraphCypherDbConnectionState(properties);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // complex reads
        registerOperationHandler(LdbcQuery1.class, LdbcQuery1Handler.class);
        registerOperationHandler(LdbcQuery2.class, LdbcQuery2Handler.class);
        registerOperationHandler(LdbcQuery3.class, LdbcQuery3Handler.class);
        registerOperationHandler(LdbcQuery4.class, LdbcQuery4Handler.class);
        registerOperationHandler(LdbcQuery5.class, LdbcQuery5Handler.class);
        registerOperationHandler(LdbcQuery6.class, LdbcQuery6Handler.class);
        registerOperationHandler(LdbcQuery7.class, LdbcQuery7Handler.class);
        registerOperationHandler(LdbcQuery8.class, LdbcQuery8Handler.class);
        registerOperationHandler(LdbcQuery9.class, LdbcQuery9Handler.class);
        registerOperationHandler(LdbcQuery10.class, LdbcQuery10Handler.class);
        registerOperationHandler(LdbcQuery11.class, LdbcQuery11Handler.class);
        registerOperationHandler(LdbcQuery12.class, LdbcQuery12Handler.class);
        registerOperationHandler(LdbcQuery13.class, LdbcQuery13Handler.class);
        registerOperationHandler(LdbcQuery14.class, LdbcQuery14Handler.class);
        
        // short reads
        registerOperationHandler(LdbcShortQuery1PersonProfile.class, LdbcShortQuery1PersonProfileHandler.class);
        registerOperationHandler(LdbcShortQuery2PersonPosts.class, LdbcShortQuery2PersonPostsHandler.class);
        registerOperationHandler(LdbcShortQuery3PersonFriends.class, LdbcShortQuery3PersonFriendsHandler.class);
        registerOperationHandler(LdbcShortQuery4MessageContent.class, LdbcShortQuery4MessageContentHandler.class);
        registerOperationHandler(LdbcShortQuery5MessageCreator.class, LdbcShortQuery5MessageCreatorHandler.class);
        registerOperationHandler(LdbcShortQuery6MessageForum.class, LdbcShortQuery6MessageForumHandler.class);
        registerOperationHandler(LdbcShortQuery7MessageReplies.class, LdbcShortQuery7MessageRepliesHandler.class);
        
        // updates
        registerOperationHandler(LdbcUpdate1AddPerson.class, LdbcUpdate1AddPersonHandler.class);
        registerOperationHandler(LdbcUpdate2AddPostLike.class, LdbcUpdate2AddPostLikeHandler.class);
        registerOperationHandler(LdbcUpdate3AddCommentLike.class, LdbcUpdate3AddCommentLikeHandler.class);
        registerOperationHandler(LdbcUpdate4AddForum.class, LdbcUpdate4AddForumHandler.class);
        registerOperationHandler(LdbcUpdate5AddForumMembership.class, LdbcUpdate5AddForumMembershipHandler.class);
        registerOperationHandler(LdbcUpdate6AddPost.class, LdbcUpdate6AddPostHandler.class);
        registerOperationHandler(LdbcUpdate7AddComment.class, LdbcUpdate7AddCommentHandler.class);
        registerOperationHandler(LdbcUpdate8AddFriendship.class, LdbcUpdate8AddFriendshipHandler.class);

    }

    @Override
    protected void onClose() throws IOException {
        dcs.close();
    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException {
        return dcs;
    }

    public static class LdbcQuery1Handler implements OperationHandler<LdbcQuery1, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery1 operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                HashSet<Long> friendIdSet = new HashSet<Long>();
                LightGraphClient client = dbConnectionState.popClient();
                String query = String.format("MATCH (person:Person {id:%d}),(friend:Person {firstName:\"%s\"}) CALL algo.shortestPath(person,friend,{relationshipQuery:\"knows\",maxHops:3}) YIELD nodeCount WITH friend,nodeCount-1 AS distance,friend.id AS friendId,friend.lastName AS friendLastName ORDER BY distance ASC, friendLastName ASC, friendId ASC LIMIT 20 WHERE distance > 0 MATCH (friend)-[:personIsLocatedIn]->(friendCity) WITH friend, friendCity, distance OPTIONAL MATCH (friend)-[sa:studyAt]->(uni)-[:organisationIsLocatedIn]->(uniCity) WITH friend, collect([uni.name, sa.classYear, uniCity.name]) AS unis, friendCity, distance OPTIONAL MATCH (friend)-[wa:workAt]->(company)-[:organisationIsLocatedIn]->(companyCountry) WITH friend, collect([company.name, wa.workFrom, companyCountry.name]) AS companies, unis, friendCity, distance RETURN friend.id AS friendId, friend.lastName AS friendLastName, distance AS distanceFromPerson, friend.birthday AS friendBirthday, friend.creationDate AS friendCreationDate, friend.gender AS friendGender, friend.browserUsed AS friendBrowserUsed, friend.locationIP AS friendLocationIp, friend.email AS friendEmails, friend.speaks AS friendLanguages, friendCity.name AS friendCityName, unis AS friendUniversities, companies AS friendCompanies ORDER BY distanceFromPerson ASC, friendLastName ASC, friendId ASC LIMIT 20",
                        operation.getPersonIdQ1(), operation.getFirstName());
                Lgraph.GraphQueryResult response = client.callCypher(query);
                int numResults = response.getResultCount();
                ArrayList<LdbcQuery1Result> results = new ArrayList<>();
                for (int i = 0; i < numResults; i ++) {
                    lgraph.Lgraph.ListOfProtoFieldData res = response.getResult(i);
                    long friendId = res.getValues(0).getInt64();
                    if (friendIdSet.contains(friendId))
                        continue;
                    String friendLastName = res.getValues(1).getStr();
                    int distanceFromPerson = (int)res.getValues(2).getInt64();
                    long friendBirthday = res.getValues(3).getInt64();
                    long friendCreationDate = res.getValues(4).getInt64();
                    String friendGender = res.getValues(5).getStr();
                    String friendBrowserUsed = res.getValues(6).getStr();
                    String friendLocationIp = res.getValues(7).getStr();
                    ArrayList<String> friendEmails = Lists.newArrayList(res.getValues(8).getStr().split(";"));
                    ArrayList<String> friendLanguages = Lists.newArrayList(res.getValues(9).getStr().split(";"));
                    String friendCityName = res.getValues(10).getStr();
                    ArrayList<LdbcQuery1Result.Organization> friendUniversities = new ArrayList<>();
                    ArrayList<LdbcQuery1Result.Organization> friendCompanies = new ArrayList<>();
                    String resStudyExpStr = res.getValues(11).getStr();
                    String resStudyExp = resStudyExpStr.substring(2, resStudyExpStr.length()-2).replaceAll("\\]\\,", "");
                    for (String studyExp : resStudyExp.split("\\[")) {
                        ArrayList<String> tmp = Lists.newArrayList(studyExp.split(","));
                        if (tmp.get(0).equals("NUL"))
                            continue;
                        int l=0;
                        StringBuilder study = new StringBuilder(tmp.get(l++));
                        for (;l<tmp.size(); l++) {
                            if (tmp.get(l).matches("\\d+"))
                                break;
                            study.append(",").append(tmp.get(l));
                        }
                        int year = Integer.parseInt(tmp.get(l++));
                        StringBuilder studyPlace = new StringBuilder(tmp.get(l++));
                        for (; l<tmp.size(); l++)
                            studyPlace.append(",").append(tmp.get(l));
                        friendUniversities.add(new LdbcQuery1Result.Organization(study.toString(), year, studyPlace.toString()));
                    }
                    String resWorkExpStr = res.getValues(12).getStr();
                    String resWorkExp = resWorkExpStr.substring(2, resWorkExpStr.length()-2).replaceAll("\\]\\,", "");
                    for (String WorkExp : resWorkExp.split("\\[")) {
                        ArrayList<String> tmp = Lists.newArrayList(WorkExp.split(","));
                        if (tmp.get(0).equals("NUL"))
                            continue;
                        int l=0;
                        StringBuilder work = new StringBuilder(tmp.get(l++));
                        for (;l<tmp.size(); l++) {
                            if (tmp.get(l).matches("\\d+"))
                                break;
                            work.append(",").append(tmp.get(l));
                        }
                        int year = Integer.parseInt(tmp.get(l++));
                        StringBuilder workPlace = new StringBuilder(tmp.get(l++));
                        for (; l<tmp.size(); l++)
                            workPlace.append(",").append(tmp.get(l));
                        friendCompanies.add(new LdbcQuery1Result.Organization(work.toString(), year, workPlace.toString()));
                    }
                    LdbcQuery1Result result = new LdbcQuery1Result(
                        friendId, friendLastName, distanceFromPerson, friendBirthday, friendCreationDate, friendGender, friendBrowserUsed, friendLocationIp,
                        friendEmails, friendLanguages, friendCityName, friendUniversities, friendCompanies
                    );
                    results.add(result);
                    friendIdSet.add(friendId);
                    if (friendIdSet.size() == 20)
                        break;
                }
                resultReporter.report(0, results, operation);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcQuery2Handler implements OperationHandler<LdbcQuery2, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery2 operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                LightGraphClient client = dbConnectionState.popClient();
                String query = String.format("MATCH (:Person {id:%d})-[:knows]-(friend)<-[:postHasCreator|commentHasCreator]-(message) WHERE message.creationDate <= %d RETURN friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, message.id AS messageId, CASE exists(message.content) WHEN true THEN message.content ELSE message.imageFile END AS messageContent, message.creationDate AS messageCreationDate ORDER BY messageCreationDate DESC, messageId ASC LIMIT 20", operation.getPersonIdQ2(), operation.getMaxDate().getTime());
                Lgraph.GraphQueryResult response = client.callCypher(query);
                int numResults = response.getResultCount();
                ArrayList<LdbcQuery2Result> results = new ArrayList<>();
                for (int i = 0; i < numResults; i ++) {
                    lgraph.Lgraph.ListOfProtoFieldData res = response.getResult(i);
                    long personId =res.getValues(0).getInt64();
                    String personFirstName = res.getValues(1).getStr();
                    String personLastName = res.getValues(2).getStr();
                    long messageId = res.getValues(3).getInt64();
                    String messageContent = res.getValues(4).getStr();
                    long messageCreationDate = res.getValues(5).getInt64();
                    LdbcQuery2Result result = new LdbcQuery2Result(
                        personId, personFirstName, personLastName,
                        messageId, messageContent, messageCreationDate
                    );
                    results.add(result);
                }
                resultReporter.report(0, results, operation);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcQuery3Handler implements OperationHandler<LdbcQuery3, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery3 operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                LightGraphClient client = dbConnectionState.popClient();
                long startTime = operation.getStartDate().getTime();
                long endTime = startTime + (long)operation.getDurationDays()*24*3600*1000;
                String query = String.format("MATCH (person:Person {id:%d})-[:knows*1..2]-(friend)<-[:postHasCreator|commentHasCreator]-(messageX)-[:postIsLocatedIn|commentIsLocatedIn]->(countryX) WHERE person.id <> friend.id AND countryX.name='%s' AND messageX.creationDate>=%d AND messageX.creationDate<%d AND not exists((friend)-[:personIsLocatedIn]->()-[:isPartOf]->(countryX)) WITH friend, count(DISTINCT messageX) AS xCount MATCH (friend)<-[:postHasCreator|commentHasCreator]-(messageY)-[:postIsLocatedIn|commentIsLocatedIn]->(countryY) WHERE countryY.name='%s' AND messageY.creationDate>=%d AND messageY.creationDate<%d AND not exists((friend)-[:personIsLocatedIn]->()-[:isPartOf]->(countryY)) WITH friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, xCount, count(DISTINCT messageY) AS yCount RETURN personId, personFirstName, personLastName, xCount, yCount, xCount + yCount AS count ORDER BY xCount DESC, personId ASC LIMIT 20", operation.getPersonIdQ3(), operation.getCountryYName(), startTime, endTime, operation.getCountryYName(), startTime, endTime);
                Lgraph.GraphQueryResult response = client.callCypher(query);
                int numResults = response.getResultCount();
                ArrayList<LdbcQuery3Result> results = new ArrayList<>();
                for (int i = 0; i < numResults; i ++) {
                    lgraph.Lgraph.ListOfProtoFieldData res = response.getResult(i);
                    long personId = res.getValues(0).getInt64();
                    String personFirstName = res.getValues(1).getStr();
                    String personLastName = res.getValues(2).getStr();
                    int xCount = (int)res.getValues(3).getInt64();
                    int yCount = (int)res.getValues(4).getInt64();
                    int count = (int)res.getValues(5).getInt64();
                    LdbcQuery3Result result = new LdbcQuery3Result(
                        personId, personFirstName, personLastName,
                        xCount, yCount, count
                    );
                    results.add(result);
                }
                resultReporter.report(0, results, operation);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcQuery4Handler implements OperationHandler<LdbcQuery4, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery4 operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                LightGraphClient client = dbConnectionState.popClient();
                long startTime = operation.getStartDate().getTime();
                long endTime = startTime + (long)operation.getDurationDays()*24*3600*1000;
//                String query = String.format("MATCH (person:Person {id:%d})-[:knows]-()<-[:postHasCreator]-(post)-[:postHasTag]->(tag) WHERE post.creationDate >= %d AND post.creationDate < %d WITH person, count(post) AS postsOnTag, tag OPTIONAL MATCH (person)-[:knows]-()<-[:postHasCreator]-(oldPost)-[r:postHasTag]->(tag) WHERE oldPost.creationDate < %d WITH person, postsOnTag, tag, count(r) AS cp WHERE cp = 0.0 RETURN tag.name AS tagName, sum(postsOnTag) AS postCount ORDER BY postCount DESC, tagName ASC LIMIT 10",
//                        operation.personId(), startTime, endTime, startTime);
                String query = String.format("MATCH (person:Person {id:%d})-[:knows]-()<-[:postHasCreator]-(post)-[:postHasTag]->(tag) WHERE post.creationDate < %d WITH count(post) AS postsOnTag, min(post.creationDate) as earliestDate, tag WHERE earliestDate >= %d RETURN tag.name AS tagName, sum(postsOnTag) AS postCount ORDER BY postCount DESC, tagName ASC LIMIT 10",
                        operation.getPersonIdQ4(), endTime, startTime);
                Lgraph.GraphQueryResult response = client.callCypher(query);
                int numResults = response.getResultCount();
                ArrayList<LdbcQuery4Result> results = new ArrayList<>();
                for (int i = 0; i < numResults; i ++) {
                    lgraph.Lgraph.ListOfProtoFieldData res = response.getResult(i);
                    String tagName = res.getValues(0).getStr();
                    int postCount = (int)res.getValues(1).getDp();
                    LdbcQuery4Result result = new LdbcQuery4Result(
                        tagName, postCount
                    );
                    results.add(result);
                }
                resultReporter.report(0, results, operation);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcQuery5Handler implements OperationHandler<LdbcQuery5, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery5 operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                LightGraphClient client = dbConnectionState.popClient();
                String query = String.format("MATCH (person:Person {id:%d})-[:knows*1..2]-(friend:Person)<-[membership:hasMember]-(forum:Forum) WHERE membership.joinDate>%d AND person.id <> friend.id WITH DISTINCT forum, friend OPTIONAL MATCH (forum)-[:containerOf]->(post:Post)-[hc:postHasCreator]->(friend) RETURN forum.id, forum.title AS forumTitle, count(hc) AS postCount ORDER BY postCount DESC, forum.id ASC LIMIT 20", operation.getPersonIdQ5(), operation.getMinDate().getTime());
                Lgraph.GraphQueryResult response = client.callCypher(query);
                int numResults = response.getResultCount();
                ArrayList<LdbcQuery5Result> results = new ArrayList<>();
                for (int i = 0; i < numResults; i ++) {
                    lgraph.Lgraph.ListOfProtoFieldData res = response.getResult(i);
                    //long forumId = res.getValues(0).getInt64();
                    String forumTitle = res.getValues(1).getStr();
                    int postCount = (int)res.getValues(2).getInt64();
                    LdbcQuery5Result result = new LdbcQuery5Result(
                        forumTitle, postCount
                    );
                    results.add(result);
                }
                resultReporter.report(0, results, operation);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcQuery6Handler implements OperationHandler<LdbcQuery6, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery6 operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                LightGraphClient client = dbConnectionState.popClient();
                String query = String.format("MATCH (person:Person {id:%d})-[:knows*1..2]-(friend)<-[:postHasCreator]-(friendPost)-[:postHasTag]->(knownTag) WHERE person.id <> friend.id AND knownTag.name = '%s' WITH DISTINCT friend, friendPost, knownTag MATCH (friendPost)-[:postHasTag]->(commonTag) WHERE commonTag.name <> '%s' WITH DISTINCT commonTag, knownTag, friend MATCH (commonTag)<-[:postHasTag]-(commonPost)-[:postHasTag]->(knownTag) WHERE commonPost.creator = id(friend) RETURN commonTag.name AS tagName, count(commonPost) AS postCount ORDER BY postCount DESC, tagName ASC LIMIT 10", operation.getPersonIdQ6(), operation.getTagName(), operation.getTagName());
                Lgraph.GraphQueryResult response = client.callCypher(query);
                int numResults = response.getResultCount();
                ArrayList<LdbcQuery6Result> results = new ArrayList<>();
                for (int i = 0; i < numResults; i ++) {
                    lgraph.Lgraph.ListOfProtoFieldData res = response.getResult(i);
                    String tagName = res.getValues(0).getStr();
                    int postCount = (int)res.getValues(1).getInt64();
                    LdbcQuery6Result result = new LdbcQuery6Result(
                        tagName, postCount
                    );
                    results.add(result);
                }
                resultReporter.report(0, results, operation);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcQuery7Handler implements OperationHandler<LdbcQuery7, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery7 operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                LightGraphClient client = dbConnectionState.popClient();
                String query = String.format("MATCH (person:Person {id:%d})<-[:commentHasCreator|postHasCreator]-(message)<-[like:likes]-(liker) WITH liker, message, message.id AS msgId, like.creationDate AS likeTime, person ORDER BY likeTime DESC, msgId ASC WITH liker, collect(likeTime) AS collLike, collect(msgId) AS collMsg, person WITH liker, head(collLike) AS likeCreationDate, head(collMsg) AS messageId, person MATCH (msg {id:messageId}) WHERE label(msg) = 'Post' OR label(msg) = 'Comment' RETURN liker.id AS personId, liker.firstName AS personFirstName, liker.lastName AS personLastName, likeCreationDate, messageId, CASE exists(msg.content) WHEN true THEN msg.content ELSE msg.imageFile END AS messageContent, msg.creationDate AS msgCreateionDate, NOT exists((liker)-[:knows]-(person)) AS isNew ORDER BY likeCreationDate DESC, personId ASC LIMIT 20", operation.getPersonIdQ7());
                Lgraph.GraphQueryResult response = client.callCypher(query);
                int numResults = response.getResultCount();
                ArrayList<LdbcQuery7Result> results = new ArrayList<>();
                for (int i = 0; i < numResults; i ++) {
                    lgraph.Lgraph.ListOfProtoFieldData res = response.getResult(i);
                    long personId = res.getValues(0).getInt64();
                    String personFirstName = res.getValues(1).getStr();
                    String personLastName = res.getValues(2).getStr();
                    long likeCreationDate = res.getValues(3).getInt64();
                    long commentOrPostId = res.getValues(4).getInt64();
                    String commentOrPostContent = res.getValues(5).getStr();
                    int minutesLatency = (int)((res.getValues(3).getInt64()-res.getValues(6).getInt64())/1000/60);
                    boolean isNew = res.getValues(7).getBoolean();
                    LdbcQuery7Result result = new LdbcQuery7Result(
                        personId, personFirstName, personLastName,
                        likeCreationDate, commentOrPostId, commentOrPostContent, minutesLatency, isNew
                    );
                    results.add(result);
                }
                resultReporter.report(0, results, operation);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcQuery8Handler implements OperationHandler<LdbcQuery8, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery8 operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                LightGraphClient client = dbConnectionState.popClient();
                String query = String.format("MATCH (:Person {id:%d})<-[:postHasCreator|commentHasCreator]-()<-[:replyOf]-(comment:Comment)-[:commentHasCreator]->(person:Person) RETURN person.id AS personId, person.firstName AS personFirstName, person.lastName AS personLastName, comment.creationDate AS commentCreationDate, comment.id AS commentId, comment.content AS commentContent ORDER BY commentCreationDate DESC, commentId ASC LIMIT 20", operation.getPersonIdQ8());
                Lgraph.GraphQueryResult response = client.callCypher(query);
                int numResults = response.getResultCount();
                ArrayList<LdbcQuery8Result> results = new ArrayList<>();
                for (int i = 0; i < numResults; i ++) {
                    lgraph.Lgraph.ListOfProtoFieldData res = response.getResult(i);
                    long personId = res.getValues(0).getInt64();
                    String personFirstName = res.getValues(1).getStr();
                    String personLastName = res.getValues(2).getStr();
                    long commentCreationDate = res.getValues(3).getInt64();
                    long commentId = res.getValues(4).getInt64();
                    String commentContent = res.getValues(5).getStr();
                    LdbcQuery8Result result = new LdbcQuery8Result(
                        personId, personFirstName, personLastName,
                        commentCreationDate, commentId, commentContent
                    );
                    results.add(result);
                }
                resultReporter.report(0, results, operation);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcQuery9Handler implements OperationHandler<LdbcQuery9, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery9 operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                LightGraphClient client = dbConnectionState.popClient();
                String query = String.format("MATCH (:Person {id:%d})-[:knows*1..2]-(friend:Person)<-[:postHasCreator|commentHasCreator]-(message) WHERE message.creationDate < %d RETURN DISTINCT friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, message.id AS messageId, CASE exists(message.content) WHEN true THEN message.content ELSE message.imageFile END AS messageContent, message.creationDate AS messageCreationDate ORDER BY message.creationDate DESC, message.id ASC LIMIT 20", operation.getPersonIdQ9(), operation.getMaxDate().getTime());
                Lgraph.GraphQueryResult response = client.callCypher(query);
                int numResults = response.getResultCount();
                ArrayList<LdbcQuery9Result> results = new ArrayList<>();
                for (int i = 0; i < numResults; i ++) {
                    lgraph.Lgraph.ListOfProtoFieldData res = response.getResult(i);
                    long personId = res.getValues(0).getInt64();
                    String personFirstName = res.getValues(1).getStr();
                    String personLastName = res.getValues(2).getStr();
                    long messageId = res.getValues(3).getInt64();
                    String messageContent = res.getValues(4).getStr();
                    long messageCreationDate = res.getValues(5).getInt64();
                    LdbcQuery9Result result = new LdbcQuery9Result(
                        personId, personFirstName, personLastName,
                        messageId, messageContent, messageCreationDate
                    );
                    results.add(result);
                }
                resultReporter.report(0, results, operation);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcQuery10Handler implements OperationHandler<LdbcQuery10, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery10 operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                LightGraphClient client = dbConnectionState.popClient();
                int month = operation.getMonth();
                int nextMonth = month % 12 + 1;
                String query = String.format("MATCH (person:Person {id:%d})-[:hasInterest]->(tag) WITH person, collect(tag.id) AS tags MATCH (person)-[:knows]-()-[:knows]-(friend:Person)-[:personIsLocatedIn]->(city) WHERE (datetimeComponent(friend.birthday, 'month') = %d AND datetimeComponent(friend.birthday, 'day') >= 21 OR datetimeComponent(friend.birthday, 'month') = %d AND datetimeComponent(friend.birthday, 'day') < 22) AND person.id <> friend.id AND not exists((friend)-[:knows]-(person)) WITH DISTINCT friend, city, person, tags OPTIONAL MATCH (friend)<-[:postHasCreator]-(post) WITH friend, city, collect(post.id)+[-1] AS posts, person, tags UNWIND posts AS postId OPTIONAL MATCH (p:Post {id:postId})-[:postHasTag]->(t) WHERE t.id IN tags WITH friend, city, size(posts)-1 AS postCount, count(DISTINCT p) AS commonPostCount RETURN friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, commonPostCount - (postCount - commonPostCount) AS commonInterestScore, friend.gender AS personGender, city.name AS personCityName ORDER BY commonInterestScore DESC, personId ASC LIMIT 10", operation.getPersonIdQ10(), month, nextMonth);
                Lgraph.GraphQueryResult response = client.callCypher(query);
                int numResults = response.getResultCount();
                ArrayList<LdbcQuery10Result> results = new ArrayList<>();
                for (int i = 0; i < numResults; i ++) {
                    lgraph.Lgraph.ListOfProtoFieldData res = response.getResult(i);
                    long personId = res.getValues(0).getInt64();
                    String personFirstName = res.getValues(1).getStr();
                    String personLastName = res.getValues(2).getStr();
                    int commonInterestScore = (int)res.getValues(3).getInt64();
                    String personGender = res.getValues(4).getStr();
                    String personCityName = res.getValues(5).getStr();
                    LdbcQuery10Result result = new LdbcQuery10Result(
                        personId, personFirstName, personLastName,
                        commonInterestScore, personGender, personCityName
                    );
                    results.add(result);
                }
                resultReporter.report(0, results, operation);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcQuery11Handler implements OperationHandler<LdbcQuery11, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery11 operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                LightGraphClient client = dbConnectionState.popClient();
                String query = String.format("MATCH (person:Person {id:%d})-[:knows*1..2]-(friend) WHERE person.id <> friend.id WITH DISTINCT friend MATCH (friend)-[workAt:workAt]->(company)-[:organisationIsLocatedIn]->(place) WHERE place.name = '%s' AND workAt.workFrom < %d RETURN friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, company.name AS organizationName, workAt.workFrom AS organizationWorkFromYear ORDER BY organizationWorkFromYear ASC, personId ASC, organizationName DESC LIMIT 10", operation.getPersonIdQ11(), operation.getCountryName(), operation.getWorkFromYear());
                Lgraph.GraphQueryResult response = client.callCypher(query);
                int numResults = response.getResultCount();
                ArrayList<LdbcQuery11Result> results = new ArrayList<>();
                for (int i = 0; i < numResults; i ++) {
                    lgraph.Lgraph.ListOfProtoFieldData res = response.getResult(i);
                    long personId = res.getValues(0).getInt64();
                    String personFirstName = res.getValues(1).getStr();
                    String personLastName = res.getValues(2).getStr();
                    String organisationName = res.getValues(3).getStr();
                    int organisationWorkFromYear = res.getValues(4).getInt32();
                    LdbcQuery11Result result = new LdbcQuery11Result(
                        personId, personFirstName, personLastName,
                        organisationName, organisationWorkFromYear
                    );  
                    results.add(result);
                }   
                resultReporter.report(0, results, operation);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcQuery12Handler implements OperationHandler<LdbcQuery12, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery12 operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                LightGraphClient client = dbConnectionState.popClient();
                String query = String.format("MATCH (:Person {id:%d})-[:knows]-(friend:Person)<-[:commentHasCreator]-(comment)-[:replyOf]->(:Post)-[:postHasTag]->(tag)-[:hasType]->(tagClass)-[:isSubclassOf*0..]->(baseTagClass) WHERE tagClass.name = '%s' OR baseTagClass.name = '%s' RETURN friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, collect(DISTINCT tag.name) AS tagNames, count(DISTINCT comment) AS replyCount ORDER BY replyCount DESC, personId ASC LIMIT 20",operation.getPersonIdQ12(),operation.getTagClassName(),operation.getTagClassName());
                Lgraph.GraphQueryResult response = client.callCypher(query);
                int numResults = response.getResultCount();
                ArrayList<LdbcQuery12Result> results = new ArrayList<>();
                for (int i = 0; i < numResults; i ++) {
                    lgraph.Lgraph.ListOfProtoFieldData res = response.getResult(i);
                    long personId = res.getValues(0).getInt64();
                    String personFirstName = res.getValues(1).getStr();
                    String personLastName = res.getValues(2).getStr();
                    String tmp1 = res.getValues(3).getStr();
                    String tmp2 = tmp1.substring(1, tmp1.length()-1);
                    ArrayList<String> tagNames = Lists.newArrayList(tmp2.split(","));
                    int replyCount = (int)res.getValues(4).getInt64();
                    LdbcQuery12Result result = new LdbcQuery12Result(
                        personId, personFirstName, personLastName,
                        tagNames, replyCount
                    );
                    results.add(result);
                }
                resultReporter.report(0, results, operation);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcQuery13Handler implements OperationHandler<LdbcQuery13, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery13 operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                LightGraphClient client = dbConnectionState.popClient();
                String query = String.format("MATCH (person1:Person {id:%d}), (person2:Person {id:%d}) CALL algo.shortestPath(person1,person2,{relationshipQuery:'knows'}) YIELD nodeCount RETURN nodeCount - 1 AS shortestPathLength", operation.getPerson1IdQ13StartNode(), operation.getPerson2IdQ13EndNode());
                Lgraph.GraphQueryResult response = client.callCypher(query);
                int numResults = response.getResultCount();
                lgraph.Lgraph.ListOfProtoFieldData res = response.getResult(0);
                int shortestPathLength = (int)res.getValues(0).getInt64();
                LdbcQuery13Result result = new LdbcQuery13Result(
                    shortestPathLength
                );
                resultReporter.report(0, result, operation);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcQuery14Handler implements OperationHandler<LdbcQuery14, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery14 operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                LightGraphClient client = dbConnectionState.popClient();
                String query = String.format("MATCH (person1:Person {id:%d}), (person2:Person {id:%d}) CALL algo.allShortestPaths(person1, person2, {relationshipQuery:'knows'}) YIELD nodeIds,relationshipIds WITH nodeIds,relationshipIds UNWIND relationshipIds AS rid CALL algo.native.extract(rid, {isNode:false, field:'weight'}) YIELD value WITH nodeIds,sum(value) AS pathWight CALL algo.native.extract(nodeIds, {isNode:true, field:'id'}) YIELD value RETURN value AS personIdsInPath, pathWight ORDER BY pathWight DESC",operation.getPerson1IdQ14StartNode(),operation.getPerson2IdQ14EndNode());
                Lgraph.GraphQueryResult response = client.callCypher(query);
                int numResults = response.getResultCount();
                ArrayList<LdbcQuery14Result> results = new ArrayList<>();
                for (int i = 0; i < numResults; i ++) {
                    lgraph.Lgraph.ListOfProtoFieldData res = response.getResult(i);
                    String tmp1 = res.getValues(0).getStr();
                    String tmp2 = tmp1.substring(1, tmp1.length()-1);
                    ArrayList<Long> personIdsInPath = new ArrayList<>();
                    for (String s : tmp2.split(",")) {
                        personIdsInPath.add(Long.parseLong(s));
                    }
                    double pathWeight = res.getValues(1).getDp();
                    LdbcQuery14Result result = new LdbcQuery14Result(personIdsInPath, pathWeight);
                    results.add(result);
                }
                resultReporter.report(0, results, operation);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcShortQuery1PersonProfileHandler implements OperationHandler<LdbcShortQuery1PersonProfile, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery1PersonProfile operation,
                LightGraphCypherDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try {
                LightGraphClient client = dbConnectionState.popClient();
                String query = String.format("MATCH (n:Person {id:%d})-[:personIsLocatedIn]->(p:Place) RETURN n.firstName AS firstName, n.lastName AS lastName, n.birthday AS birthday, n.locationIP AS locationIP, n.browserUsed AS browserUsed, p.id AS cityId, n.gender AS gender, n.creationDate AS creationDate",operation.getPersonIdSQ1());
                Lgraph.GraphQueryResult response = client.callCypher(query);
                int numResults = response.getResultCount();
                lgraph.Lgraph.ListOfProtoFieldData res = response.getResult(0);
                String firstName = res.getValues(0).getStr();
                String lastName = res.getValues(1).getStr();
                long birthday = res.getValues(2).getInt64();
                String locationIP = res.getValues(3).getStr();
                String browserUsed = res.getValues(4).getStr();
                long cityId = res.getValues(5).getInt64();
                String gender = res.getValues(6).getStr();
                long creationDate = res.getValues(7).getInt64();
                LdbcShortQuery1PersonProfileResult result = new LdbcShortQuery1PersonProfileResult(
                    firstName, lastName, birthday, locationIP, browserUsed, cityId, gender, creationDate
                );
                resultReporter.report(0, result, operation);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcShortQuery2PersonPostsHandler implements OperationHandler<LdbcShortQuery2PersonPosts, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery2PersonPosts operation,
                LightGraphCypherDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try {
                LightGraphClient client = dbConnectionState.popClient();
                String query = String.format("MATCH (:Person {id:%d})<-[:commentHasCreator|postHasCreator]-(m)-[:replyOf*0..]->(p:Post) WITH m,p MATCH (p)-[:postHasCreator]->(c) RETURN m.id as messageId, CASE exists(m.content) WHEN true THEN m.content ELSE m.imageFile END AS messageContent, m.creationDate AS messageCreationDate, p.id AS originalPostId, c.id AS originalPostAuthorId, c.firstName as originalPostAuthorFirstName, c.lastName as originalPostAuthorLastName ORDER BY messageCreationDate DESC LIMIT 10", operation.getPersonIdSQ2());
                Lgraph.GraphQueryResult response = client.callCypher(query);
                int numResults = response.getResultCount();
                ArrayList<LdbcShortQuery2PersonPostsResult> results = new ArrayList<>();
                for (int i = 0; i < numResults; i ++) {
                    lgraph.Lgraph.ListOfProtoFieldData res = response.getResult(i);
                    long messageId = res.getValues(0).getInt64();
                    String messageContent = res.getValues(1).getStr();
                    long messageCreationDate = res.getValues(2).getInt64();
                    long originalPostId = res.getValues(3).getInt64();
                    long originalPostAuthorId = res.getValues(4).getInt64();
                    String originalPostAuthorFirstName = res.getValues(5).getStr();
                    String originalPostAuthorLastName = res.getValues(6).getStr();
                    LdbcShortQuery2PersonPostsResult result = new LdbcShortQuery2PersonPostsResult(
                        messageId, messageContent, messageCreationDate,
                        originalPostId, originalPostAuthorId, originalPostAuthorFirstName, originalPostAuthorLastName
                    );
                    results.add(result);
                }
                resultReporter.report(0, results, operation);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcShortQuery3PersonFriendsHandler implements OperationHandler<LdbcShortQuery3PersonFriends, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery3PersonFriends operation,
                LightGraphCypherDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try {
                LightGraphClient client = dbConnectionState.popClient();
                String query = String.format("MATCH (n:Person {id:%d})-[r:knows]-(friend) RETURN friend.id AS personId, friend.firstName AS firstName, friend.lastName AS lastName, r.creationDate AS friendshipCreationDate ORDER BY friendshipCreationDate DESC, personId ASC", operation.getPersonIdSQ3());
                Lgraph.GraphQueryResult response = client.callCypher(query);
                int numResults = response.getResultCount();
                ArrayList<LdbcShortQuery3PersonFriendsResult> results = new ArrayList<>();
                for (int i = 0; i < numResults; i ++) {
                    lgraph.Lgraph.ListOfProtoFieldData res = response.getResult(i);
                    long personId = res.getValues(0).getInt64();
                    String firstName = res.getValues(1).getStr();
                    String lastName = res.getValues(2).getStr();
                    long friendshipCreationDate = res.getValues(3).getInt64();
                    LdbcShortQuery3PersonFriendsResult result = new LdbcShortQuery3PersonFriendsResult(
                        personId, firstName, lastName, friendshipCreationDate
                    );
                    results.add(result);
                }
                resultReporter.report(0, results, operation);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcShortQuery4MessageContentHandler implements OperationHandler<LdbcShortQuery4MessageContent, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery4MessageContent operation,
                LightGraphCypherDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try {
                LightGraphClient client = dbConnectionState.popClient();
                String query = String.format("MATCH (m {id:%d}) WHERE label(m) = 'Post' OR label(m) = 'Comment' RETURN m.creationDate as messageCreationDate, CASE exists(m.content) WHEN true THEN m.content ELSE m.imageFile END AS messageContent", operation.getMessageIdContent());
                Lgraph.GraphQueryResult response = client.callCypher(query);
                int numResults = response.getResultCount();
                lgraph.Lgraph.ListOfProtoFieldData res = response.getResult(0);
                long messageCreationDate = res.getValues(0).getInt64();
                String messageContent = res.getValues(1).getStr();
                LdbcShortQuery4MessageContentResult result = new LdbcShortQuery4MessageContentResult(
                    messageContent, messageCreationDate
                );
                resultReporter.report(0, result, operation);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcShortQuery5MessageCreatorHandler implements OperationHandler<LdbcShortQuery5MessageCreator, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery5MessageCreator operation,
                LightGraphCypherDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try {
                LightGraphClient client = dbConnectionState.popClient();
                String query = String.format("MATCH (m {id:%d})-[:commentHasCreator|postHasCreator]->(p) RETURN p.id AS personId, p.firstName AS firstName, p.lastName AS lastName", operation.getMessageIdCreator());
                Lgraph.GraphQueryResult response = client.callCypher(query);
                int numResults = response.getResultCount();
                lgraph.Lgraph.ListOfProtoFieldData res = response.getResult(0);
                long personId = res.getValues(0).getInt64();
                String firstName = res.getValues(1).getStr();
                String lastName = res.getValues(2).getStr();
                LdbcShortQuery5MessageCreatorResult result = new LdbcShortQuery5MessageCreatorResult(
                    personId, firstName, lastName
                );
                resultReporter.report(0, result, operation);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcShortQuery6MessageForumHandler implements OperationHandler<LdbcShortQuery6MessageForum, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery6MessageForum operation,
                LightGraphCypherDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try {
                LightGraphClient client = dbConnectionState.popClient();
                String query = String.format("MATCH (m {id:%d})-[:replyOf*0..]->(p:Post)<-[:containerOf]-(f)-[:hasModerator]->(mod) RETURN f.id AS forumId, f.title AS forumTitle, mod.id AS moderatorId, mod.firstName AS moderatorFirstName, mod.lastName AS moderatorLastName", operation.getMessageForumId());
                Lgraph.GraphQueryResult response = client.callCypher(query);
                int numResults = response.getResultCount();
                lgraph.Lgraph.ListOfProtoFieldData res = response.getResult(0);
                long forumId = res.getValues(0).getInt64();
                String forumTitle = res.getValues(1).getStr();
                long moderatorId = res.getValues(2).getInt64();
                String moderatorFirstName = res.getValues(3).getStr();
                String moderatorLastName = res.getValues(4).getStr();
                LdbcShortQuery6MessageForumResult result = new LdbcShortQuery6MessageForumResult(
                    forumId, forumTitle, moderatorId, moderatorFirstName, moderatorLastName
                );
                resultReporter.report(0, result, operation);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcShortQuery7MessageRepliesHandler implements OperationHandler<LdbcShortQuery7MessageReplies, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery7MessageReplies operation,
                LightGraphCypherDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try {
                LightGraphClient client = dbConnectionState.popClient();
                String query = String.format("MATCH (m {id:%d})<-[:replyOf]-(c:Comment)-[:commentHasCreator]->(p) WITH m,c,p OPTIONAL MATCH (m)-[:commentHasCreator|postHasCreator]->(a)-[r:knows]-(p) RETURN c.id AS commentId, c.content AS commentContent, c.creationDate AS commentCreationDate, p.id AS replyAuthorId, p.firstName AS replyAuthorFirstName, p.lastName AS replyAuthorLastName, CASE r WHEN null THEN false ELSE true END AS replyAuthorKnowsOriginalMessageAuthor ORDER BY commentCreationDate DESC, replyAuthorId", operation.getMessageRepliesId());
                Lgraph.GraphQueryResult response = client.callCypher(query);
                int numResults = response.getResultCount();
                ArrayList<LdbcShortQuery7MessageRepliesResult> results = new ArrayList<>();
                for (int i = 0; i < numResults; i ++) {
                    lgraph.Lgraph.ListOfProtoFieldData res = response.getResult(i);
                    long commentId = res.getValues(0).getInt64();
                    String commentContent = res.getValues(1).getStr();
                    long commentCreationDate = res.getValues(2).getInt64();
                    long replyAuthorId = res.getValues(3).getInt64();
                    String replyAuthorFirstName = res.getValues(4).getStr();
                    String replyAuthorLastName = res.getValues(5).getStr();
                    boolean replyAuthorKnowsOriginalMessageAuthor = res.getValues(6).getBoolean();
                    LdbcShortQuery7MessageRepliesResult result = new LdbcShortQuery7MessageRepliesResult(
                        commentId, commentContent, commentCreationDate,
                        replyAuthorId, replyAuthorFirstName, replyAuthorLastName,
                        replyAuthorKnowsOriginalMessageAuthor
                    );
                    results.add(result);
                }
                resultReporter.report(0, results, operation);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcUpdate1AddPersonHandler implements OperationHandler<LdbcUpdate1AddPerson, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate1AddPerson operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                LightGraphClient client = dbConnectionState.popClient();
                String query = String.format("MATCH (c:Place {id:%d}) CREATE (p:Person {id: %d, firstName: '%s', lastName: \"%s\", gender: '%s', birthday: %d, creationDate: %d, locationIP: '%s', browserUsed: '%s', place:id(c), speaks: '%s', email: '%s'})-[:personIsLocatedIn]->(c) WITH p UNWIND %s AS tagId MATCH (t:Tag {id: tagId}) CREATE (p)-[:hasInterest ]->(t)",
                        operation.getCityId(), operation.getPersonId(), operation.getPersonFirstName(), operation.getPersonLastName(), operation.getGender(),
                        operation.getBirthday().getTime(), operation.getCreationDate().getTime(), operation.getLocationIp(), operation.getBrowserUsed(),
                        String.join(";", operation.getLanguages()), String.join(";", operation.getEmails()), operation.getTagIds());
                Lgraph.GraphQueryResult response = client.callCypher(query);
//                dbConnectionState.logUpdate(String.format("[\"LdbcUpdate1AddPerson\",%d,%s,%s,%s,%d,%d,%s,%s,%d,%s,%s,%s]|%s\n",
//                        operation.personId(), operation.personFirstName(), operation.personLastName(), operation.gender(), operation.birthday().getTime(),
//                        operation.creationDate().getTime(), operation.locationIp(), operation.browserUsed(), operation.cityId(),String.join(";", operation.languages()),
//                        String.join(";", operation.emails()), operation.tagIds(), response.getResult(0).getValues(0).getStr()));
                for (LdbcUpdate1AddPerson.Organization org : operation.getStudyAt()) {
                    String loop = String.format("MATCH (p:Person {id: %d}),(u:Organisation {id: %d}) CREATE (p)-[:studyAt {classYear: %d}]->(u)",
                            operation.getPersonId(), org.getOrganizationId(), org.getYear());
                    Lgraph.GraphQueryResult r = client.callCypher(loop);
//                    dbConnectionState.logUpdate(String.format("[\"LdbcUpdate1AddPerson\",%d,%s,%s,%s,%d,%d,%s,%s,%d,%s,%s,%s studyAt[year:%d,id:%d]]|%s\n",
//                            operation.personId(), operation.personFirstName(), operation.personLastName(), operation.gender(), operation.birthday().getTime(),
//                            operation.creationDate().getTime(), operation.locationIp(), operation.browserUsed(), operation.cityId(),String.join(";", operation.languages()),
//                            String.join(";", operation.emails()), operation.tagIds(), org.year(), org.organizationId(), r.getResult(0).getValues(0).getStr()));
                }
                for (LdbcUpdate1AddPerson.Organization org : operation.getWorkAt()) {
                    String loop = String.format("MATCH (p:Person {id: %d}),(comp:Organisation {id: %d}) CREATE (p)-[:workAt {workFrom: %d}]->(comp)",
                            operation.getPersonId(), org.getOrganizationId(), org.getYear());
                    Lgraph.GraphQueryResult r = client.callCypher(loop);
//                    dbConnectionState.logUpdate(String.format("[\"LdbcUpdate1AddPerson\",%d,%s,%s,%s,%d,%d,%s,%s,%d,%s,%s,%s workAt[year:%d,id:%d]]|%s\n",
//                            operation.personId(), operation.personFirstName(), operation.personLastName(), operation.gender(), operation.birthday().getTime(),
//                            operation.creationDate().getTime(), operation.locationIp(), operation.browserUsed(), operation.cityId(),String.join(";", operation.languages()),
//                            String.join(";", operation.emails()), operation.tagIds(), org.year(), org.organizationId(), r.getResult(0).getValues(0).getStr()));
                }
                resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcUpdate2AddPostLikeHandler implements OperationHandler<LdbcUpdate2AddPostLike, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate2AddPostLike operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                LightGraphClient client = dbConnectionState.popClient();
                String query = String.format("MATCH (person:Person {id:%d}),(post:Post {id:%d}) CREATE (person)-[:likes {creationDate:%d}]->(post)", operation.getPersonId(), operation.getPostId(), operation.getCreationDate().getTime());
                Lgraph.GraphQueryResult response = client.callCypher(query);
//                dbConnectionState.logUpdate(String.format("[\"LdbcUpdate2AddPostLike\",%d,%d,%d]|%s\n",
//                        operation.personId(), operation.postId(), operation.creationDate().getTime(), response.getResult(0).getValues(0).getStr()));
                resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcUpdate3AddCommentLikeHandler implements OperationHandler<LdbcUpdate3AddCommentLike, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate3AddCommentLike operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                LightGraphClient client = dbConnectionState.popClient();
                String query = String.format("MATCH (person:Person {id:%d}),(comment:Comment {id:%d}) CREATE (person)-[:likes {creationDate:%d}]->(comment)", operation.getPersonId(), operation.getCommentId(), operation.getCreationDate().getTime());
                Lgraph.GraphQueryResult response = client.callCypher(query);
//                dbConnectionState.logUpdate(String.format("[\"LdbcUpdate3AddCommentLike\",%d,%d,%d]|%s\n",
//                        operation.personId(), operation.commentId(), operation.creationDate().getTime(), response.getResult(0).getValues(0).getStr()));
                resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcUpdate4AddForumHandler implements OperationHandler<LdbcUpdate4AddForum, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate4AddForum operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                LightGraphClient client = dbConnectionState.popClient();
                String query = String.format("MATCH (p:Person {id: %d}) CREATE (f:Forum {id: %d, title: \"%s\", creationDate: %d, moderator:id(p)})-[:hasModerator]->(p) WITH f UNWIND %s AS tagId MATCH (t:Tag {id: tagId}) CREATE (f)-[:forumHasTag]->(t)", 
                        operation.getModeratorPersonId(), operation.getForumId(), operation.getForumTitle(), operation.getCreationDate().getTime(), operation.getTagIds());
                Lgraph.GraphQueryResult response = client.callCypher(query);
//                dbConnectionState.logUpdate(String.format("[\"LdbcUpdate4AddForum\",%d,%s,%d,%d,%s]|%s\n",
//                        operation.forumId(), operation.forumTitle(), operation.creationDate().getTime(), operation.moderatorPersonId(), operation.tagIds(), response.getResult(0).getValues(0).getStr()));
                resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcUpdate5AddForumMembershipHandler implements OperationHandler<LdbcUpdate5AddForumMembership, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate5AddForumMembership operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                LightGraphClient client = dbConnectionState.popClient();
                String query = String.format("MATCH (f:Forum {id:%d}), (p:Person {id:%d}) WITH f,p OPTIONAL MATCH (f)-[:containerOf]->(post:Post)-[hc:postHasCreator]->(p) WITH f,p,count(hc) AS postCount CREATE (f)-[:hasMember {joinDate:%d, numPosts:postCount, forumId:%d}]->(p)",
                        operation.getForumId(), operation.getPersonId(), operation.getJoinDate().getTime(), operation.getForumId());
                Lgraph.GraphQueryResult response = client.callCypher(query);
//                dbConnectionState.logUpdate(String.format("[\"LdbcUpdate5AddForumMembership\",%d,%d,%d]|%s\n",
//                        operation.forumId(), operation.personId(), operation.joinDate().getTime(), response.getResult(0).getValues(0).getStr()));
                resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcUpdate6AddPostHandler implements OperationHandler<LdbcUpdate6AddPost, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate6AddPost operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                LightGraphClient client = dbConnectionState.popClient();
                String query = "";
                if (!operation.getContent().isEmpty()) {
                    query = String.format("MATCH (author:Person {id: %d}), (country:Place {id: %d}), (forum:Forum {id: %d}) CREATE (author)<-[:postHasCreator {creationDate:%d}]-(p:Post {id: %d, creationDate: %d, locationIP: '%s', browserUsed: '%s', content: \"%s\", length: %d, creator:id(author), container:id(forum), place:id(country)})<-[:containerOf]-(forum), (p)-[:postIsLocatedIn {creationDate:%d}]->(country)",
                        operation.getAuthorPersonId(), operation.getCountryId(), operation.getForumId(), operation.getCreationDate().getTime(), operation.getPostId(),
                        operation.getCreationDate().getTime(), operation.getLocationIp(), operation.getBrowserUsed(), operation.getContent(), operation.getLength(),
                        operation.getCreationDate().getTime());
                }
                else if (!operation.getImageFile().isEmpty()) {
                    query = String.format("MATCH (author:Person {id: %d}), (country:Place {id: %d}), (forum:Forum {id: %d}) CREATE (author)<-[:postHasCreator {creationDate:%d}]-(p:Post {id: %d, creationDate: %d, locationIP: '%s', browserUsed: '%s', imageFile: \"%s\", length: %d, creator:id(author), container:id(forum), place:id(country)})<-[:containerOf]-(forum), (p)-[:postIsLocatedIn {creationDate:%d}]->(country)",
                        operation.getAuthorPersonId(), operation.getCountryId(), operation.getForumId(), operation.getCreationDate().getTime(), operation.getPostId(),
                        operation.getCreationDate().getTime(), operation.getLocationIp(), operation.getBrowserUsed(), operation.getImageFile(), operation.getLength(),
                        operation.getCreationDate().getTime());
                }
                String query2 = "";
                if (!operation.getTagIds().isEmpty())
                    query2 = String.format(" WITH p,author,forum UNWIND %s AS tagId MATCH (t:Tag {id: tagId}) CREATE (p)-[:postHasTag]->(t) WITH author,forum,count(tagId) AS dummy MATCH (forum)-[m:hasMember]->(author) SET m.numPosts = m.numPosts+1", operation.getTagIds());
                else
                    query2 = " WITH author,forum MATCH (forum)-[m:hasMember]->(author) SET m.numPosts = m.numPosts+1";
                query = query + query2;
                Lgraph.GraphQueryResult response = client.callCypher(query);
//                dbConnectionState.logUpdate(String.format("[\"LdbcUpdate6AddPost\",%d,%s,%d,%s,%s,%s,%d,%d,%d,%d,%s]|%s\n",
//                        operation.postId(), operation.imageFile(), operation.creationDate().getTime(), operation.locationIp(), operation.browserUsed(),
//                        operation.content(), operation.length(), operation.authorPersonId(), operation.forumId(), operation.countryId(),
//                        operation.tagIds(), response.getResult(0).getValues(0).getStr()));
                resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcUpdate7AddCommentHandler implements OperationHandler<LdbcUpdate7AddComment, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate7AddComment operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                LightGraphClient client = dbConnectionState.popClient();
                long msgId = operation.getReplyToPostId() + operation.getReplyToCommentId() + 1;
                double weight = 0.0;
                String replyOf = "";
                String postOrComment = "";
                if (operation.getReplyToPostId() != -1) {
                    weight = 1.0;
                    replyOf = ",replyOfPost:id(message)";
                    postOrComment = ":Post";
                }
                else if (operation.getReplyToCommentId() != -1) {
                    weight = 0.5;
                    replyOf = ",replyOfComment:id(message)";
                    postOrComment = ":Comment";
                }
                else
                    System.out.println("\n\n qwqw : Update7 ERROR\n\n");
                String query = String.format("MATCH (author:Person {id: %d}), (country:Place {id: %d}), (message%s {id: %d}) CREATE (author)<-[:commentHasCreator {creationDate:%d}]-(c:Comment {id: %d, creationDate: %d, locationIP: '%s', browserUsed: '%s', content: \"%s\", length: %d, creator:id(author),place:id(country)%s})-[:replyOf {creationDate:%d}]->(message), (c)-[:commentIsLocatedIn {creationDate:%d}]->(country) WITH c, author, message UNWIND %s AS tagId MATCH (t:Tag {id: tagId}) CREATE (c)-[:commentHasTag]->(t) WITH author, message, count(tagId) AS dummy MATCH (message)-[:commentHasCreator|postHasCreator]->(p2) WITH author, p2 MATCH (author)-[r:knows]-(p2) SET r.weight = r.weight + %f",
                    operation.getAuthorPersonId(), operation.getCountryId(), postOrComment, msgId, operation.getCreationDate().getTime(), operation.getCommentId(),
                    operation.getCreationDate().getTime(), operation.getLocationIp(), operation.getBrowserUsed(), operation.getContent(), operation.getLength(),
                    replyOf, operation.getCreationDate().getTime(), operation.getCreationDate().getTime(), operation.getTagIds(),
                    weight);
                if (operation.getTagIds().isEmpty())
                    query = String.format("MATCH (author:Person {id: %d}), (country:Place {id: %d}), (message%s {id: %d}) CREATE (author)<-[:commentHasCreator {creationDate:%d}]-(c:Comment {id: %d, creationDate: %d, locationIP: '%s', browserUsed: '%s', content: \"%s\", length: %d, creator:id(author),place:id(country)%s})-[:replyOf {creationDate:%d}]->(message), (c)-[:commentIsLocatedIn {creationDate:%d}]->(country) WITH author, message MATCH (message)-[:commentHasCreator|postHasCreator]->(p2) WITH author, p2 MATCH (author)-[r:knows]-(p2) SET r.weight = r.weight + %f",
                    operation.getAuthorPersonId(), operation.getCountryId(), postOrComment, msgId, operation.getCreationDate().getTime(), operation.getCommentId(),
                    operation.getCreationDate().getTime(), operation.getLocationIp(), operation.getBrowserUsed(), operation.getContent(), operation.getLength(),
                    replyOf, operation.getCreationDate().getTime(), operation.getCreationDate().getTime(),
                    weight);
                Lgraph.GraphQueryResult response = client.callCypher(query);
//                dbConnectionState.logUpdate(String.format("[\"LdbcUpdate7AddComment\",%d,%d,%s,%s,%s,%d,%d,%d,%d,%d,%s]|%s\n",
//                        operation.commentId(), operation.creationDate().getTime(), operation.locationIp(), operation.browserUsed(), operation.content(),
//                        operation.length(), operation.authorPersonId(), operation.countryId(), operation.replyToPostId(), operation.replyToCommentId(),
//                        operation.tagIds(), response.getResult(0).getValues(0).getStr()));
                resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcUpdate8AddFriendshipHandler implements OperationHandler<LdbcUpdate8AddFriendship, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate8AddFriendship operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                LightGraphClient client = dbConnectionState.popClient();
                String query = String.format("MATCH (p1:Person {id:%d}), (p2:Person {id:%d}) WITH p1, p2 OPTIONAL MATCH (p1)<-[:commentHasCreator]-(c)-[:replyOf]->(:Post)-[hc:postHasCreator]->(p2) WITH p1, p2, count(hc) AS replyOnPost1 OPTIONAL MATCH (p1)<-[:commentHasCreator]-(c)-[:replyOf]->(:Comment)-[hc:commentHasCreator]->(p2) WITH p1, p2, replyOnPost1, count(hc) AS replyOnComment1 OPTIONAL MATCH (p2)<-[:commentHasCreator]-(c)-[:replyOf]->(:Post)-[hc:postHasCreator]->(p1) WITH p1, p2, replyOnPost1, replyOnComment1, count(hc) AS replyOnPost2 OPTIONAL MATCH (p2)<-[:commentHasCreator]-(c)-[:replyOf]->(:Comment)-[hc:commentHasCreator]->(p1) WITH p1, p2, replyOnPost1, replyOnComment1, replyOnPost2, count(hc) AS replyOnComment2 CREATE (p1)-[:knows {creationDate:%d, weight:replyOnPost1*1.0+replyOnPost2*1.0+replyOnComment1*0.5+replyOnComment2*0.5}]->(p2)",
                        operation.getPerson1Id(), operation.getPerson2Id(), operation.getCreationDate().getTime());
                Lgraph.GraphQueryResult response = client.callCypher(query);
//                dbConnectionState.logUpdate(String.format("[\"LdbcUpdate8AddFriendship\",%d,%d,%d]|%s\n",
//                        operation.person1Id(), operation.person2Id(), operation.creationDate().getTime(), response.getResult(0).getValues(0).getStr()));
                resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
