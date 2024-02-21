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

public class LightGraphInteractiveCypherDb extends Db {

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
                CloseableHttpClient client = dbConnectionState.getClient();
                String endpoint = dbConnectionState.getEndpoint();
                String token = dbConnectionState.getToken();
                HttpPost request = new HttpPost("http://" + endpoint + "/cypher");
                request.setHeader("Accept", "application/json");
                request.setHeader("Authorization", "Bearer "+token);
                String input = String.format("{\"graph\":\"default\", \"script\":\"MATCH path = (:Person {id:%d})-[:knows*1..2]-(friend) WHERE friend.firstName = '%s' WITH friend, friend.lastName AS friendLastName, friend.id AS friendId, min(length(path)) AS distance ORDER BY distance ASC, friendLastName ASC, friendId ASC LIMIT 20 MATCH (friend)-[:personIsLocatedIn]->(friendCity) WITH friend, friendCity, distance OPTIONAL MATCH (friend)-[sa:studyAt]->(uni)-[:organisationIsLocatedIn]->(uniCity) WITH friend, collect([uni.name, sa.classYear, uniCity.name]) AS unis, friendCity, distance OPTIONAL MATCH (friend)-[wa:workAt]->(company)-[:organisationIsLocatedIn]->(companyCountry) WITH friend, collect([company.name, wa.workFrom, companyCountry.name]) AS companies, unis, friendCity, distance RETURN friend.id AS friendId, friend.lastName AS friendLastName, distance AS distanceFromPerson, friend.birthday AS friendBirthday, friend.creationDate AS friendCreationDate, friend.gender AS friendGender, friend.browserUsed AS friendBrowserUsed, friend.locationIP AS friendLocationIp, friend.email AS friendEmails, friend.speaks AS friendLanguages, friendCity.name AS friendCityName, unis AS friendUniversities, companies AS friendCompanies ORDER BY distanceFromPerson ASC, friendLastName ASC, friendId ASC LIMIT 20\"}", operation.getPersonIdQ1(), operation.getFirstName());
                request.setEntity(new StringEntity(input, ContentType.APPLICATION_JSON));
                CloseableHttpResponse response1 = client.execute(request);
                JSONObject jsonObject1 = new JSONObject(EntityUtils.toString(response1.getEntity()));
                int numResults1 = jsonObject1.getInt("size");
                JSONArray resArray1 = jsonObject1.getJSONArray("result");
                int numResults2 = 0;
                JSONArray resArray2 = new JSONArray();
                if (numResults1 < 20) {
                    input = String.format("{\"graph\":\"default\", \"script\":\"MATCH path = (:Person {id:%d})-[:knows]-()-[:knows]-()-[:knows]-(friend) WHERE friend.firstName = '%s' WITH friend, friend.lastName AS friendLastName, friend.id AS friendId, min(length(path)) AS distance ORDER BY distance ASC, friendLastName ASC, friendId ASC LIMIT 20 MATCH (friend)-[:personIsLocatedIn]->(friendCity) WITH friend, friendCity, distance OPTIONAL MATCH (friend)-[sa:studyAt]->(uni)-[:organisationIsLocatedIn]->(uniCity) WITH friend, collect([uni.name, sa.classYear, uniCity.name]) AS unis, friendCity, distance OPTIONAL MATCH (friend)-[wa:workAt]->(company)-[:organisationIsLocatedIn]->(companyCountry) WITH friend, collect([company.name, wa.workFrom, companyCountry.name]) AS companies, unis, friendCity, distance RETURN friend.id AS friendId, friend.lastName AS friendLastName, distance AS distanceFromPerson, friend.birthday AS friendBirthday, friend.creationDate AS friendCreationDate, friend.gender AS friendGender, friend.browserUsed AS friendBrowserUsed, friend.locationIP AS friendLocationIp, friend.email AS friendEmails, friend.speaks AS friendLanguages, friendCity.name AS friendCityName, unis AS friendUniversities, companies AS friendCompanies ORDER BY distanceFromPerson ASC, friendLastName ASC, friendId ASC LIMIT 20\"}", operation.getPersonIdQ1(), operation.getFirstName());
                    request.setEntity(new StringEntity(input, ContentType.APPLICATION_JSON));
                    CloseableHttpResponse response2 = client.execute(request);
                    JSONObject jsonObject2 = new JSONObject(EntityUtils.toString(response2.getEntity()));
                    numResults2 = jsonObject2.getInt("size");
                    resArray2 = jsonObject2.getJSONArray("result");
                }
                int numResults = numResults1 + numResults2;
                ArrayList<LdbcQuery1Result> results = new ArrayList<>();
                for (int i = 0; i < numResults; i ++) {
                    JSONArray res = new JSONArray();
                    if (i < numResults1)
                        res = resArray1.getJSONArray(i);
                    else
                        res = resArray2.getJSONArray(i-numResults1);
                    long friendId = res.getLong(0);
                    if (friendIdSet.contains(friendId))
                        continue;
                    String friendLastName = res.getString(1);
                    int distanceFromPerson = res.getInt(2);
                    long friendBirthday = res.getLong(3);
                    long friendCreationDate = res.getLong(4);
                    String friendGender = res.getString(5);
                    String friendBrowserUsed = res.getString(6);
                    String friendLocationIp = res.getString(7);
                    ArrayList<String> friendEmails = Lists.newArrayList(res.getString(8).split(";"));
                    ArrayList<String> friendLanguages = Lists.newArrayList(res.getString(9).split(";"));
                    String friendCityName = res.getString(10);
                    ArrayList<LdbcQuery1Result.Organization> friendUniversities = new ArrayList<>();
                    ArrayList<LdbcQuery1Result.Organization> friendCompanies = new ArrayList<>();
                    String resStudyExpStr = res.getString(11);
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
                    String resWorkExpStr = res.getString(12);
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
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcQuery2Handler implements OperationHandler<LdbcQuery2, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery2 operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            CloseableHttpClient client = dbConnectionState.getClient();
            String endpoint = dbConnectionState.getEndpoint();
            String token = dbConnectionState.getToken();
            HttpPost request = new HttpPost("http://" + endpoint + "/cypher");
            request.setHeader("Accept", "application/json");
            request.setHeader("Authorization", "Bearer "+token);
            String input = String.format("{\"graph\":\"default\", \"script\":\"MATCH (:Person {id:%d})-[:knows]-(friend)<-[:postHasCreator|commentHasCreator]-(message) WHERE message.creationDate <= %d RETURN friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, message.id AS messageId, CASE exists(message.content) WHEN true THEN message.content ELSE message.imageFile END AS messageContent, message.creationDate AS messageCreationDate ORDER BY messageCreationDate DESC, messageId ASC LIMIT 20\"}", operation.getPersonIdQ2(), operation.getMaxDate().getTime());
            request.setEntity(new StringEntity(input, ContentType.APPLICATION_JSON));
            try {
                CloseableHttpResponse response = client.execute(request);
                JSONObject jsonObject = new JSONObject(EntityUtils.toString(response.getEntity()));
                int numResults = jsonObject.getInt("size");
                JSONArray resArray = jsonObject.getJSONArray("result");
                ArrayList<LdbcQuery2Result> results = new ArrayList<>();
                for (int i = 0; i < numResults; i ++) {
                    JSONArray res = resArray.getJSONArray(i);
                    long personId = res.getLong(0);
                    String personFirstName = res.getString(1);
                    String personLastName = res.getString(2);
                    long messageId = res.getLong(3);
                    String messageContent = res.getString(4);
                    long messageCreationDate = res.getLong(5);
                    LdbcQuery2Result result = new LdbcQuery2Result(
                        personId, personFirstName, personLastName,
                        messageId, messageContent, messageCreationDate
                    );
                    results.add(result);
                }
                resultReporter.report(0, results, operation);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcQuery3Handler implements OperationHandler<LdbcQuery3, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery3 operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            CloseableHttpClient client = dbConnectionState.getClient();
            String endpoint = dbConnectionState.getEndpoint();
            String token = dbConnectionState.getToken();
            HttpPost request = new HttpPost("http://" + endpoint + "/cypher");
            request.setHeader("Accept", "application/json");
            request.setHeader("Authorization", "Bearer "+token);
            long startTime = operation.getStartDate().getTime();
            long endTime = startTime + (long)operation.getDurationDays()*24*3600*1000;
            String input = String.format("{\"graph\":\"default\", \"script\":\"MATCH (person:Person {id:%d})-[:knows*1..2]-(friend)<-[:postHasCreator|commentHasCreator]-(messageX)-[:postIsLocatedIn|commentIsLocatedIn]->(countryX) WHERE person.id <> friend.id AND countryX.name='%s' AND messageX.creationDate>=%d AND messageX.creationDate<%d AND not exists((friend)-[:personIsLocatedIn]->()-[:isPartOf]->(countryX)) WITH friend, count(DISTINCT messageX) AS xCount MATCH (friend)<-[:postHasCreator|commentHasCreator]-(messageY)-[:postIsLocatedIn|commentIsLocatedIn]->(countryY) WHERE countryY.name='%s' AND messageY.creationDate>=%d AND messageY.creationDate<%d AND not exists((friend)-[:personIsLocatedIn]->()-[:isPartOf]->(countryY)) WITH friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, xCount, count(DISTINCT messageY) AS yCount RETURN personId, personFirstName, personLastName, xCount, yCount, xCount + yCount AS count ORDER BY xCount DESC, personId ASC LIMIT 20\"}", operation.getPersonIdQ3(), operation.getCountryXName(), startTime, endTime, operation.getCountryYName(), startTime, endTime);
            request.setEntity(new StringEntity(input, ContentType.APPLICATION_JSON));
            try {
                CloseableHttpResponse response = client.execute(request);
                JSONObject jsonObject = new JSONObject(EntityUtils.toString(response.getEntity()));
                int numResults = jsonObject.getInt("size");
                JSONArray resArray = jsonObject.getJSONArray("result");
                ArrayList<LdbcQuery3Result> results = new ArrayList<>();
                for (int i = 0; i < numResults; i ++) {
                    JSONArray res = resArray.getJSONArray(i);
                    long personId = res.getLong(0);
                    String personFirstName = res.getString(1);
                    String personLastName = res.getString(2);
                    int xCount = res.getInt(3);
                    int yCount = res.getInt(4);
                    int count = res.getInt(5);
                    LdbcQuery3Result result = new LdbcQuery3Result(
                        personId, personFirstName, personLastName,
                        xCount, yCount, count
                    );
                    results.add(result);
                }
                resultReporter.report(0, results, operation);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcQuery4Handler implements OperationHandler<LdbcQuery4, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery4 operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            CloseableHttpClient client = dbConnectionState.getClient();
            String endpoint = dbConnectionState.getEndpoint();
            String token = dbConnectionState.getToken();
            HttpPost request = new HttpPost("http://" + endpoint + "/cypher");
            request.setHeader("Accept", "application/json");
            request.setHeader("Authorization", "Bearer "+token);
            long startTime = operation.getStartDate().getTime();
            long endTime = startTime + (long)operation.getDurationDays()*24*3600*1000;
            String input = String.format("{\"graph\":\"default\", \"script\":\"MATCH (person:Person {id:%d})-[:knows]-()<-[:postHasCreator]-(post)-[:postHasTag]->(tag) WHERE post.creationDate >= %d AND post.creationDate < %d WITH person, count(post) AS postsOnTag, tag OPTIONAL MATCH (person)-[:knows]-()<-[:postHasCreator]-(oldPost)-[r:postHasTag]->(tag) WHERE oldPost.creationDate < %d WITH person, postsOnTag, tag, count(r) AS cp WHERE cp = 0.0 RETURN tag.name AS tagName, sum(postsOnTag) AS postCount ORDER BY postCount DESC, tagName ASC LIMIT 10\"}", operation.getPersonIdQ4(), startTime, endTime, startTime);
            request.setEntity(new StringEntity(input, ContentType.APPLICATION_JSON));
            try {
                CloseableHttpResponse response = client.execute(request);
                JSONObject jsonObject = new JSONObject(EntityUtils.toString(response.getEntity()));
                int numResults = jsonObject.getInt("size");
                JSONArray resArray = jsonObject.getJSONArray("result");
                ArrayList<LdbcQuery4Result> results = new ArrayList<>();
                for (int i = 0; i < numResults; i ++) {
                    JSONArray res = resArray.getJSONArray(i);
                    String tagName = res.getString(0);
                    int postCount = res.getInt(1);
                    LdbcQuery4Result result = new LdbcQuery4Result(
                        tagName, postCount
                    );
                    results.add(result);
                }
                resultReporter.report(0, results, operation);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcQuery5Handler implements OperationHandler<LdbcQuery5, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery5 operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            CloseableHttpClient client = dbConnectionState.getClient();
            String endpoint = dbConnectionState.getEndpoint();
            String token = dbConnectionState.getToken();
            HttpPost request = new HttpPost("http://" + endpoint + "/cypher");
            request.setHeader("Accept", "application/json");
            request.setHeader("Authorization", "Bearer "+token);
            String input = String.format("{\"graph\":\"default\", \"script\":\"MATCH (person:Person {id:%d})-[:knows*1..2]-(friend:Person)<-[membership:hasMember]-(forum:Forum) WHERE membership.joinDate>%d AND person.id <> friend.id WITH DISTINCT forum, friend OPTIONAL MATCH (forum)-[:containerOf]->(post:Post)-[hc:postHasCreator]->(friend) RETURN forum.id, forum.title AS forumTitle, count(hc) AS postCount ORDER BY postCount DESC, forum.id ASC LIMIT 20\"}", operation.getPersonIdQ5(), operation.getMinDate().getTime());
            request.setEntity(new StringEntity(input, ContentType.APPLICATION_JSON));
            try {
                CloseableHttpResponse response = client.execute(request);
                JSONObject jsonObject = new JSONObject(EntityUtils.toString(response.getEntity()));
                int numResults = jsonObject.getInt("size");
                JSONArray resArray = jsonObject.getJSONArray("result");
                ArrayList<LdbcQuery5Result> results = new ArrayList<>();
                for (int i = 0; i < numResults; i ++) {
                    JSONArray res = resArray.getJSONArray(i);
                    //long forumId = res.getLong(0);
                    String forumTitle = res.getString(1);
                    int postCount = res.getInt(2);
                    LdbcQuery5Result result = new LdbcQuery5Result(
                        forumTitle, postCount
                    );
                    results.add(result);
                }
                resultReporter.report(0, results, operation);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcQuery6Handler implements OperationHandler<LdbcQuery6, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery6 operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            CloseableHttpClient client = dbConnectionState.getClient();
            String endpoint = dbConnectionState.getEndpoint();
            String token = dbConnectionState.getToken();
            HttpPost request = new HttpPost("http://" + endpoint + "/cypher");
            request.setHeader("Accept", "application/json");
            request.setHeader("Authorization", "Bearer "+token);
            String input = String.format("{\"graph\":\"default\", \"script\":\"MATCH (person:Person {id:%d})-[:knows*1..2]-(friend)<-[:postHasCreator]-(friendPost)-[:postHasTag]->(knownTag) WHERE person.id <> friend.id AND knownTag.name = '%s' WITH DISTINCT friend, friendPost, knownTag MATCH (friendPost)-[:postHasTag]->(commonTag) WHERE commonTag.name <> '%s' WITH DISTINCT commonTag, knownTag, friend MATCH (commonTag)<-[:postHasTag]-(commonPost)-[:postHasTag]->(knownTag) WHERE commonPost.creator = friend.id RETURN commonTag.name AS tagName, count(commonPost) AS postCount ORDER BY postCount DESC, tagName ASC LIMIT 10\"}", operation.getPersonIdQ6(), operation.getTagName(), operation.getTagName());
            request.setEntity(new StringEntity(input, ContentType.APPLICATION_JSON));
            try {
                CloseableHttpResponse response = client.execute(request);
                JSONObject jsonObject = new JSONObject(EntityUtils.toString(response.getEntity()));
                int numResults = jsonObject.getInt("size");
                JSONArray resArray = jsonObject.getJSONArray("result");
                ArrayList<LdbcQuery6Result> results = new ArrayList<>();
                for (int i = 0; i < numResults; i ++) {
                    JSONArray res = resArray.getJSONArray(i);
                    String tagName = res.getString(0);
                    int postCount = res.getInt(1);
                    LdbcQuery6Result result = new LdbcQuery6Result(
                        tagName, postCount
                    );
                    results.add(result);
                }
                resultReporter.report(0, results, operation);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcQuery7Handler implements OperationHandler<LdbcQuery7, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery7 operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            CloseableHttpClient client = dbConnectionState.getClient();
            String endpoint = dbConnectionState.getEndpoint();
            String token = dbConnectionState.getToken();
            HttpPost request = new HttpPost("http://" + endpoint + "/cypher");
            request.setHeader("Accept", "application/json");
            request.setHeader("Authorization", "Bearer "+token);
            String input = String.format("{\"graph\":\"default\", \"script\":\"MATCH (person:Person {id:%d})<-[:commentHasCreator|postHasCreator]-(message)<-[like:likes]-(liker) WITH liker, message, message.id AS msgId, like.creationDate AS likeTime, person ORDER BY likeTime DESC, msgId ASC WITH liker, collect(likeTime) AS collLike, collect(msgId) AS collMsg, person WITH liker, head(collLike) AS likeCreationDate, head(collMsg) AS messageId, person MATCH (msg {id:messageId}) WHERE label(msg) = 'Post' OR label(msg) = 'Comment' RETURN liker.id AS personId, liker.firstName AS personFirstName, liker.lastName AS personLastName, likeCreationDate, messageId, CASE exists(msg.content) WHEN true THEN msg.content ELSE msg.imageFile END AS messageContent, msg.creationDate AS msgCreateionDate, NOT exists((liker)-[:knows]-(person)) AS isNew ORDER BY likeCreationDate DESC, personId ASC LIMIT 20\"}", operation.getPersonIdQ7());
            request.setEntity(new StringEntity(input, ContentType.APPLICATION_JSON));
            try {
                CloseableHttpResponse response = client.execute(request);
                JSONObject jsonObject = new JSONObject(EntityUtils.toString(response.getEntity()));
                int numResults = jsonObject.getInt("size");
                JSONArray resArray = jsonObject.getJSONArray("result");
                ArrayList<LdbcQuery7Result> results = new ArrayList<>();
                for (int i = 0; i < numResults; i ++) {
                    JSONArray res = resArray.getJSONArray(i);
                    long personId = res.getLong(0);
                    String personFirstName = res.getString(1);
                    String personLastName = res.getString(2);
                    long likeCreationDate = res.getLong(3);
                    long commentOrPostId = res.getLong(4);
                    String commentOrPostContent = res.getString(5);
                    int minutesLatency = (int)((res.getLong(3)-res.getLong(6))/1000/60);
                    boolean isNew = res.getBoolean(7);
                    LdbcQuery7Result result = new LdbcQuery7Result(
                        personId, personFirstName, personLastName,
                        likeCreationDate, commentOrPostId, commentOrPostContent, minutesLatency, isNew
                    );
                    results.add(result);
                }
                resultReporter.report(0, results, operation);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcQuery8Handler implements OperationHandler<LdbcQuery8, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery8 operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            CloseableHttpClient client = dbConnectionState.getClient();
            String endpoint = dbConnectionState.getEndpoint();
            String token = dbConnectionState.getToken();
            HttpPost request = new HttpPost("http://" + endpoint + "/cypher");
            request.setHeader("Accept", "application/json");
            request.setHeader("Authorization", "Bearer "+token);
            String input = String.format("{\"graph\":\"default\", \"script\":\"MATCH (:Person {id:%d})<-[:postHasCreator|commentHasCreator]-()<-[:replyOf]-(comment:Comment)-[:commentHasCreator]->(person:Person) RETURN person.id AS personId, person.firstName AS personFirstName, person.lastName AS personLastName, comment.creationDate AS commentCreationDate, comment.id AS commentId, comment.content AS commentContent ORDER BY commentCreationDate DESC, commentId ASC LIMIT 20\"}", operation.getPersonIdQ8());
            request.setEntity(new StringEntity(input, ContentType.APPLICATION_JSON));
            try {
                CloseableHttpResponse response = client.execute(request);
                JSONObject jsonObject = new JSONObject(EntityUtils.toString(response.getEntity()));
                int numResults = jsonObject.getInt("size");
                JSONArray resArray = jsonObject.getJSONArray("result");
                ArrayList<LdbcQuery8Result> results = new ArrayList<>();
                for (int i = 0; i < numResults; i ++) {
                    JSONArray res = resArray.getJSONArray(i);
                    long personId = res.getLong(0);
                    String personFirstName = res.getString(1);
                    String personLastName = res.getString(2);
                    long commentCreationDate = res.getLong(3);
                    long commentId = res.getLong(4);
                    String commentContent = res.getString(5);
                    LdbcQuery8Result result = new LdbcQuery8Result(
                        personId, personFirstName, personLastName,
                        commentCreationDate, commentId, commentContent
                    );
                    results.add(result);
                }
                resultReporter.report(0, results, operation);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcQuery9Handler implements OperationHandler<LdbcQuery9, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery9 operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            CloseableHttpClient client = dbConnectionState.getClient();
            String endpoint = dbConnectionState.getEndpoint();
            String token = dbConnectionState.getToken();
            HttpPost request = new HttpPost("http://" + endpoint + "/cypher");
            request.setHeader("Accept", "application/json");
            request.setHeader("Authorization", "Bearer "+token);
            String input = String.format("{\"graph\":\"default\", \"script\":\"MATCH (:Person {id:%d})-[:knows*1..2]-(friend:Person)<-[:postHasCreator|commentHasCreator]-(message) WHERE message.creationDate < %d RETURN DISTINCT friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, message.id AS messageId, CASE exists(message.content) WHEN true THEN message.content ELSE message.imageFile END AS messageContent, message.creationDate AS messageCreationDate ORDER BY message.creationDate DESC, message.id ASC LIMIT 20\"}", operation.getPersonIdQ9(), operation.getMaxDate().getTime());
            request.setEntity(new StringEntity(input, ContentType.APPLICATION_JSON));
            try {
                CloseableHttpResponse response = client.execute(request);
                JSONObject jsonObject = new JSONObject(EntityUtils.toString(response.getEntity()));
                int numResults = jsonObject.getInt("size");
                JSONArray resArray = jsonObject.getJSONArray("result");
                ArrayList<LdbcQuery9Result> results = new ArrayList<>();
                for (int i = 0; i < numResults; i ++) {
                    JSONArray res = resArray.getJSONArray(i);
                    long personId = res.getLong(0);
                    String personFirstName = res.getString(1);
                    String personLastName = res.getString(2);
                    long messageId = res.getLong(3);
                    String messageContent = res.getString(4);
                    long messageCreationDate = res.getLong(5);
                    LdbcQuery9Result result = new LdbcQuery9Result(
                        personId, personFirstName, personLastName,
                        messageId, messageContent, messageCreationDate
                    );
                    results.add(result);
                }
                resultReporter.report(0, results, operation);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcQuery10Handler implements OperationHandler<LdbcQuery10, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery10 operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            CloseableHttpClient client = dbConnectionState.getClient();
            String endpoint = dbConnectionState.getEndpoint();
            String token = dbConnectionState.getToken();
            HttpPost request = new HttpPost("http://" + endpoint + "/cypher");
            request.setHeader("Accept", "application/json");
            request.setHeader("Authorization", "Bearer "+token);
            int month = operation.getMonth();
            int nextMonth = month % 12 + 1;
            //String input = String.format("{\"graph\":\"default\", \"script\":\"MATCH (person:Person {id:%d})-[:knows]-()-[:knows]-(friend:Person)-[:personIsLocatedIn]->(city) WHERE (datetimeComponent(friend.birthday, 'month') = %d AND datetimeComponent(friend.birthday, 'day') >= 21 OR datetimeComponent(friend.birthday, 'month') = %d AND datetimeComponent(friend.birthday, 'day') < 22) AND person.id <> friend.id AND not exists((friend)-[:knows]-(person)) WITH DISTINCT friend, city, person OPTIONAL MATCH (friend)<-[:postHasCreator]-(post) WITH friend, city, collect(post.id)+[-1] AS posts, person UNWIND posts AS postId OPTIONAL MATCH (p:Post {id:postId})-[:postHasTag]->()<-[:hasInterest]-(person) WITH friend, city, size(posts)-1 AS postCount, count(DISTINCT p) AS commonPostCount RETURN friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, commonPostCount - (postCount - commonPostCount) AS commonInterestScore, friend.gender AS personGender, city.name AS personCityName ORDER BY commonInterestScore DESC, personId ASC LIMIT 10\"}", operation.personId(), month, nextMonth);
            String input = String.format("{\"graph\":\"default\", \"script\":\"MATCH (person:Person {id:%d})-[:hasInterest]->(tag) WITH person, collect(tag.id) AS tags MATCH (person)-[:knows]-()-[:knows]-(friend:Person)-[:personIsLocatedIn]->(city) WHERE (datetimeComponent(friend.birthday, 'month') = %d AND datetimeComponent(friend.birthday, 'day') >= 21 OR datetimeComponent(friend.birthday, 'month') = %d AND datetimeComponent(friend.birthday, 'day') < 22) AND person.id <> friend.id AND not exists((friend)-[:knows]-(person)) WITH DISTINCT friend, city, person, tags OPTIONAL MATCH (friend)<-[:postHasCreator]-(post) WITH friend, city, collect(post.id)+[-1] AS posts, person, tags UNWIND posts AS postId OPTIONAL MATCH (p:Post {id:postId})-[:postHasTag]->(t) WHERE t.id IN tags WITH friend, city, size(posts)-1 AS postCount, count(DISTINCT p) AS commonPostCount RETURN friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, commonPostCount - (postCount - commonPostCount) AS commonInterestScore, friend.gender AS personGender, city.name AS personCityName ORDER BY commonInterestScore DESC, personId ASC LIMIT 10\"}", operation.getPersonIdQ10(), month, nextMonth);
            request.setEntity(new StringEntity(input, ContentType.APPLICATION_JSON));
            try {
                CloseableHttpResponse response = client.execute(request);
                JSONObject jsonObject = new JSONObject(EntityUtils.toString(response.getEntity()));
                int numResults = jsonObject.getInt("size");
                JSONArray resArray = jsonObject.getJSONArray("result");
                ArrayList<LdbcQuery10Result> results = new ArrayList<>();
                for (int i = 0; i < numResults; i ++) {
                    JSONArray res = resArray.getJSONArray(i);
                    long personId = res.getLong(0);
                    String personFirstName = res.getString(1);
                    String personLastName = res.getString(2);
                    int commonInterestScore = res.getInt(3);
                    String personGender = res.getString(4);
                    String personCityName = res.getString(5);
                    LdbcQuery10Result result = new LdbcQuery10Result(
                        personId, personFirstName, personLastName,
                        commonInterestScore, personGender, personCityName
                    );
                    results.add(result);
                }
                resultReporter.report(0, results, operation);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcQuery11Handler implements OperationHandler<LdbcQuery11, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery11 operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            CloseableHttpClient client = dbConnectionState.getClient();
            String endpoint = dbConnectionState.getEndpoint();
            String token = dbConnectionState.getToken();
            HttpPost request = new HttpPost("http://" + endpoint + "/cypher");
            request.setHeader("Accept", "application/json");
            request.setHeader("Authorization", "Bearer "+token);
            String input = String.format("{\"graph\":\"default\", \"script\":\"MATCH (person:Person {id:%d})-[:knows*1..2]-(friend) WHERE person.id <> friend.id WITH DISTINCT friend MATCH (friend)-[workAt:workAt]->(company)-[:organisationIsLocatedIn]->(place) WHERE place.name = '%s' AND workAt.workFrom < %d RETURN friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, company.name AS organizationName, workAt.workFrom AS organizationWorkFromYear ORDER BY organizationWorkFromYear ASC, personId ASC, organizationName DESC LIMIT 10\"}", operation.getPersonIdQ11(), operation.getCountryName(), operation.getWorkFromYear());
            request.setEntity(new StringEntity(input, ContentType.APPLICATION_JSON));
            try {
                CloseableHttpResponse response = client.execute(request);
                JSONObject jsonObject = new JSONObject(EntityUtils.toString(response.getEntity()));
                int numResults = jsonObject.getInt("size");
                JSONArray resArray = jsonObject.getJSONArray("result");
                ArrayList<LdbcQuery11Result> results = new ArrayList<>();
                for (int i = 0; i < numResults; i ++) {
                    JSONArray res = resArray.getJSONArray(i);
                    long personId = res.getLong(0);
                    String personFirstName = res.getString(1);
                    String personLastName = res.getString(2);
                    String organisationName = res.getString(3);
                    int organisationWorkFromYear = res.getInt(4);
                    LdbcQuery11Result result = new LdbcQuery11Result(
                        personId, personFirstName, personLastName,
                        organisationName, organisationWorkFromYear
                    );
                    results.add(result);
                }
                resultReporter.report(0, results, operation);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcQuery12Handler implements OperationHandler<LdbcQuery12, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery12 operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            CloseableHttpClient client = dbConnectionState.getClient();
            String endpoint = dbConnectionState.getEndpoint();
            String token = dbConnectionState.getToken();
            HttpPost request = new HttpPost("http://" + endpoint + "/cypher");
            request.setHeader("Accept", "application/json");
            request.setHeader("Authorization", "Bearer "+token);
            String input = String.format("{\"graph\":\"default\", \"script\":\"MATCH (:Person {id:%d})-[:knows]-(friend:Person)<-[:commentHasCreator]-(comment)-[:replyOf]->(:Post)-[:postHasTag]->(tag)-[:hasType]->(tagClass)-[:isSubclassOf*0..]->(baseTagClass) WHERE tagClass.name = '%s' OR baseTagClass.name = '%s' RETURN friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, collect(DISTINCT tag.name) AS tagNames, count(DISTINCT comment) AS replyCount ORDER BY replyCount DESC, personId ASC LIMIT 20\"}",operation.getPersonIdQ12(),operation.getTagClassName(),operation.getTagClassName());
            request.setEntity(new StringEntity(input, ContentType.APPLICATION_JSON));
            try {
                CloseableHttpResponse response = client.execute(request);
                JSONObject jsonObject = new JSONObject(EntityUtils.toString(response.getEntity()));
                int numResults = jsonObject.getInt("size");
                JSONArray resArray = jsonObject.getJSONArray("result");
                ArrayList<LdbcQuery12Result> results = new ArrayList<>();
                for (int i = 0; i < numResults; i ++) {
                    JSONArray res = resArray.getJSONArray(i);
                    long personId = res.getLong(0);
                    String personFirstName = res.getString(1);
                    String personLastName = res.getString(2);
                    String tmp1 = res.getString(3);
                    String tmp2 = tmp1.substring(1, tmp1.length()-1);
                    ArrayList<String> tagNames = Lists.newArrayList(tmp2.split(","));
                    int replyCount = res.getInt(4);
                    LdbcQuery12Result result = new LdbcQuery12Result(
                        personId, personFirstName, personLastName,
                        tagNames, replyCount
                    );
                    results.add(result);
                }
                resultReporter.report(0, results, operation);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcQuery13Handler implements OperationHandler<LdbcQuery13, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery13 operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            CloseableHttpClient client = dbConnectionState.getClient();
            String endpoint = dbConnectionState.getEndpoint();
            String token = dbConnectionState.getToken();
            HttpPost request = new HttpPost("http://" + endpoint + "/cypher");
            request.setHeader("Accept", "application/json");
            request.setHeader("Authorization", "Bearer "+token);
            String input = String.format("{\"graph\":\"default\", \"script\":\"MATCH (person1:Person {id:%d}), (person2:Person {id:%d}) CALL algo.shortestPath(person1,person2,{relationshipQuery:'knows'}) YIELD nodeCount RETURN nodeCount - 1 AS shortestPathLength\"}", operation.getPerson1IdQ13StartNode(), operation.getPerson2IdQ13EndNode());
            request.setEntity(new StringEntity(input, ContentType.APPLICATION_JSON));
            try {
                CloseableHttpResponse response = client.execute(request);
                JSONObject jsonObject = new JSONObject(EntityUtils.toString(response.getEntity()));
                JSONArray resArray = jsonObject.getJSONArray("result");
                JSONArray res = resArray.getJSONArray(0);
                int shortestPathLength = res.getInt(0);
                LdbcQuery13Result result = new LdbcQuery13Result(
                    shortestPathLength
                );
                resultReporter.report(0, result, operation);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcQuery14Handler implements OperationHandler<LdbcQuery14, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery14 operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            CloseableHttpClient client = dbConnectionState.getClient();
            String endpoint = dbConnectionState.getEndpoint();
            String token = dbConnectionState.getToken();
            HttpPost request = new HttpPost("http://" + endpoint + "/cypher");
            request.setHeader("Accept", "application/json");
            request.setHeader("Authorization", "Bearer "+token);
            String input = String.format("{\"graph\":\"default\", \"script\":\"MATCH (person1:Person {id:%d}), (person2:Person {id:%d}) CALL algo.allShortestPaths(person1, person2, {relationshipQuery:'knows'}) YIELD nodeIds,relationshipIds WITH nodeIds,relationshipIds UNWIND relationshipIds AS rid CALL algo.native.extract(rid, {isNode:false, field:'weight'}) YIELD value WITH nodeIds,sum(value) AS pathWight CALL algo.native.extract(nodeIds, {isNode:true, field:'id'}) YIELD value RETURN value AS personIdsInPath, pathWight ORDER BY pathWight DESC\"}",operation.getPerson1IdQ14StartNode(),operation.getPerson2IdQ14EndNode());
            request.setEntity(new StringEntity(input, ContentType.APPLICATION_JSON));
            try {
                CloseableHttpResponse response = client.execute(request);
                JSONObject jsonObject = new JSONObject(EntityUtils.toString(response.getEntity()));
                int numResults = jsonObject.getInt("size");
                JSONArray resArray = jsonObject.getJSONArray("result");
                ArrayList<LdbcQuery14Result> results = new ArrayList<>();
                for (int i = 0; i < numResults; i ++) {
                    JSONArray res = resArray.getJSONArray(i);
                    String tmp1 = res.getString(0);
                    String tmp2 = tmp1.substring(1, tmp1.length()-1);
                    ArrayList<Long> personIdsInPath = new ArrayList<>();
                    for (String s : tmp2.split(",")) {
                        personIdsInPath.add(Long.parseLong(s));
                    }
                    double pathWeight = res.getDouble(1);
                    LdbcQuery14Result result = new LdbcQuery14Result(personIdsInPath, pathWeight);
                    results.add(result);
                }
                resultReporter.report(0, results, operation);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcShortQuery1PersonProfileHandler implements OperationHandler<LdbcShortQuery1PersonProfile, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery1PersonProfile operation,
                LightGraphCypherDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            CloseableHttpClient client = dbConnectionState.getClient();
            String endpoint = dbConnectionState.getEndpoint();
            String token = dbConnectionState.getToken();
            HttpPost request = new HttpPost("http://" + endpoint + "/cypher");
            request.setHeader("Accept", "application/json");
            request.setHeader("Authorization", "Bearer "+token);
            String input = String.format("{\"graph\":\"default\", \"script\":\"MATCH (n:Person {id:%d})-[:personIsLocatedIn]->(p:Place) RETURN n.firstName AS firstName, n.lastName AS lastName, n.birthday AS birthday, n.locationIP AS locationIP, n.browserUsed AS browserUsed, p.id AS cityId, n.gender AS gender, n.creationDate AS creationDate\"}",operation.getPersonIdSQ1());
            request.setEntity(new StringEntity(input, ContentType.APPLICATION_JSON));
            try {
                CloseableHttpResponse response = client.execute(request);
                JSONObject jsonObject = new JSONObject(EntityUtils.toString(response.getEntity()));
                JSONArray res = jsonObject.getJSONArray("result").getJSONArray(0);
                String firstName = res.getString(0);
                String lastName = res.getString(1);
                long birthday = res.getLong(2);
                String locationIP = res.getString(3);
                String browserUsed = res.getString(4);
                long cityId = res.getLong(5);
                String gender = res.getString(6);
                long creationDate = res.getLong(7);
                LdbcShortQuery1PersonProfileResult result = new LdbcShortQuery1PersonProfileResult(
                    firstName, lastName, birthday, locationIP, browserUsed, cityId, gender, creationDate
                );
                resultReporter.report(0, result, operation);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcShortQuery2PersonPostsHandler implements OperationHandler<LdbcShortQuery2PersonPosts, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery2PersonPosts operation,
                LightGraphCypherDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            CloseableHttpClient client = dbConnectionState.getClient();
            String endpoint = dbConnectionState.getEndpoint();
            String token = dbConnectionState.getToken();
            HttpPost request = new HttpPost("http://" + endpoint + "/cypher");
            request.setHeader("Accept", "application/json");
            request.setHeader("Authorization", "Bearer "+token);
            String input = String.format("{\"graph\":\"default\", \"script\":\"MATCH (:Person {id:%d})<-[:commentHasCreator|postHasCreator]-(m)-[:replyOf*0..]->(p:Post) WITH m,p MATCH (p)-[:postHasCreator]->(c) RETURN m.id as messageId, CASE exists(m.content) WHEN true THEN m.content ELSE m.imageFile END AS messageContent, m.creationDate AS messageCreationDate, p.id AS originalPostId, c.id AS originalPostAuthorId, c.firstName as originalPostAuthorFirstName, c.lastName as originalPostAuthorLastName ORDER BY messageCreationDate DESC LIMIT 10\"}", operation.getPersonIdSQ2());
            request.setEntity(new StringEntity(input, ContentType.APPLICATION_JSON));
            try {
                CloseableHttpResponse response = client.execute(request);
                JSONObject jsonObject = new JSONObject(EntityUtils.toString(response.getEntity()));
                int numResults = jsonObject.getInt("size");
                JSONArray resArray = jsonObject.getJSONArray("result");
                ArrayList<LdbcShortQuery2PersonPostsResult> results = new ArrayList<>();
                for (int i = 0; i < numResults; i ++) {
                    JSONArray res = resArray.getJSONArray(i);
                    
                    long messageId = res.getLong(0);
                    String messageContent = res.getString(1);
                    long messageCreationDate = res.getLong(2);
                    long originalPostId = res.getLong(3);
                    long originalPostAuthorId = res.getLong(4);
                    String originalPostAuthorFirstName = res.getString(5);
                    String originalPostAuthorLastName = res.getString(6);
                    LdbcShortQuery2PersonPostsResult result = new LdbcShortQuery2PersonPostsResult(
                        messageId, messageContent, messageCreationDate,
                        originalPostId, originalPostAuthorId, originalPostAuthorFirstName, originalPostAuthorLastName
                    );
                    results.add(result);
                }
                resultReporter.report(0, results, operation);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcShortQuery3PersonFriendsHandler implements OperationHandler<LdbcShortQuery3PersonFriends, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery3PersonFriends operation,
                LightGraphCypherDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            CloseableHttpClient client = dbConnectionState.getClient();
            String endpoint = dbConnectionState.getEndpoint();
            String token = dbConnectionState.getToken();
            HttpPost request = new HttpPost("http://" + endpoint + "/cypher");
            request.setHeader("Accept", "application/json");
            request.setHeader("Authorization", "Bearer "+token);
            String input = String.format("{\"graph\":\"default\", \"script\":\"MATCH (n:Person {id:%d})-[r:knows]-(friend) RETURN friend.id AS personId, friend.firstName AS firstName, friend.lastName AS lastName, r.creationDate AS friendshipCreationDate ORDER BY friendshipCreationDate DESC, personId ASC\"}", operation.getPersonIdSQ3());
            request.setEntity(new StringEntity(input, ContentType.APPLICATION_JSON));
            try {
                CloseableHttpResponse response = client.execute(request);
                JSONObject jsonObject = new JSONObject(EntityUtils.toString(response.getEntity()));
                int numResults = jsonObject.getInt("size");
                JSONArray resArray = jsonObject.getJSONArray("result");
                ArrayList<LdbcShortQuery3PersonFriendsResult> results = new ArrayList<>();
                for (int i = 0; i < numResults; i ++) {
                    JSONArray res = resArray.getJSONArray(i);
                    long personId = res.getLong(0);
                    String firstName = res.getString(1);
                    String lastName = res.getString(2);
                    long friendshipCreationDate = res.getLong(3);
                    LdbcShortQuery3PersonFriendsResult result = new LdbcShortQuery3PersonFriendsResult(
                        personId, firstName, lastName, friendshipCreationDate
                    );
                    results.add(result);
                }
                resultReporter.report(0, results, operation);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcShortQuery4MessageContentHandler implements OperationHandler<LdbcShortQuery4MessageContent, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery4MessageContent operation,
                LightGraphCypherDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            CloseableHttpClient client = dbConnectionState.getClient();
            String endpoint = dbConnectionState.getEndpoint();
            String token = dbConnectionState.getToken();
            HttpPost request = new HttpPost("http://" + endpoint + "/cypher");
            request.setHeader("Accept", "application/json");
            request.setHeader("Authorization", "Bearer "+token);
            String input = String.format("{\"graph\":\"default\", \"script\":\"MATCH (m {id:%d}) WHERE label(m) = 'Post' OR label(m) = 'Comment' RETURN m.creationDate as messageCreationDate, CASE exists(m.content) WHEN true THEN m.content ELSE m.imageFile END AS messageContent\"}", operation.getMessageIdContent());
            request.setEntity(new StringEntity(input, ContentType.APPLICATION_JSON));
            try {
                CloseableHttpResponse response = client.execute(request);
                JSONObject jsonObject = new JSONObject(EntityUtils.toString(response.getEntity()));
                JSONArray res = jsonObject.getJSONArray("result").getJSONArray(0);
                long messageCreationDate = res.getLong(0);
                String messageContent = res.getString(1);
                LdbcShortQuery4MessageContentResult result = new LdbcShortQuery4MessageContentResult(
                    messageContent, messageCreationDate
                );
                resultReporter.report(0, result, operation);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcShortQuery5MessageCreatorHandler implements OperationHandler<LdbcShortQuery5MessageCreator, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery5MessageCreator operation,
                LightGraphCypherDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            CloseableHttpClient client = dbConnectionState.getClient();
            String endpoint = dbConnectionState.getEndpoint();
            String token = dbConnectionState.getToken();
            HttpPost request = new HttpPost("http://" + endpoint + "/cypher");
            request.setHeader("Accept", "application/json");
            request.setHeader("Authorization", "Bearer "+token);
            String input = String.format("{\"graph\":\"default\", \"script\":\"MATCH (m {id:%d})-[:commentHasCreator|postHasCreator]->(p) RETURN p.id AS personId, p.firstName AS firstName, p.lastName AS lastName\"}", operation.getMessageIdCreator());
            request.setEntity(new StringEntity(input, ContentType.APPLICATION_JSON));
            try {
                CloseableHttpResponse response = client.execute(request);
                JSONObject jsonObject = new JSONObject(EntityUtils.toString(response.getEntity()));
                JSONArray res = jsonObject.getJSONArray("result").getJSONArray(0);
                long personId = res.getLong(0);
                String firstName = res.getString(1);
                String lastName = res.getString(2);
                LdbcShortQuery5MessageCreatorResult result = new LdbcShortQuery5MessageCreatorResult(
                    personId, firstName, lastName
                );
                resultReporter.report(0, result, operation);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcShortQuery6MessageForumHandler implements OperationHandler<LdbcShortQuery6MessageForum, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery6MessageForum operation,
                LightGraphCypherDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            CloseableHttpClient client = dbConnectionState.getClient();
            String endpoint = dbConnectionState.getEndpoint();
            String token = dbConnectionState.getToken();
            HttpPost request = new HttpPost("http://" + endpoint + "/cypher");
            request.setHeader("Accept", "application/json");
            request.setHeader("Authorization", "Bearer "+token);
            String input = String.format("{\"graph\":\"default\", \"script\":\"MATCH (m {id:%d})-[:replyOf*0..]->(p:Post)<-[:containerOf]-(f)-[:hasModerator]->(mod) RETURN f.id AS forumId, f.title AS forumTitle, mod.id AS moderatorId, mod.firstName AS moderatorFirstName, mod.lastName AS moderatorLastName\"}", operation.getMessageForumId());
            request.setEntity(new StringEntity(input, ContentType.APPLICATION_JSON));
            try {
                CloseableHttpResponse response = client.execute(request);
                JSONObject jsonObject = new JSONObject(EntityUtils.toString(response.getEntity()));
                JSONArray res = jsonObject.getJSONArray("result").getJSONArray(0);
                long forumId = res.getLong(0);
                String forumTitle = res.getString(1);
                long moderatorId = res.getLong(2);
                String moderatorFirstName = res.getString(3);
                String moderatorLastName = res.getString(4);
                LdbcShortQuery6MessageForumResult result = new LdbcShortQuery6MessageForumResult(
                    forumId, forumTitle, moderatorId, moderatorFirstName, moderatorLastName
                );
                resultReporter.report(0, result, operation);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcShortQuery7MessageRepliesHandler implements OperationHandler<LdbcShortQuery7MessageReplies, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery7MessageReplies operation,
                LightGraphCypherDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            CloseableHttpClient client = dbConnectionState.getClient();
            String endpoint = dbConnectionState.getEndpoint();
            String token = dbConnectionState.getToken();
            HttpPost request = new HttpPost("http://" + endpoint + "/cypher");
            request.setHeader("Accept", "application/json");
            request.setHeader("Authorization", "Bearer "+token);
            String input = String.format("{\"graph\":\"default\", \"script\":\"MATCH (m {id:%d})<-[:replyOf]-(c:Comment)-[:commentHasCreator]->(p) WITH m,c,p OPTIONAL MATCH (m)-[:commentHasCreator|postHasCreator]->(a)-[r:knows]-(p) RETURN c.id AS commentId, c.content AS commentContent, c.creationDate AS commentCreationDate, p.id AS replyAuthorId, p.firstName AS replyAuthorFirstName, p.lastName AS replyAuthorLastName, CASE r WHEN null THEN false ELSE true END AS replyAuthorKnowsOriginalMessageAuthor ORDER BY commentCreationDate DESC, replyAuthorId\"}", operation.getMessageRepliesId());
            request.setEntity(new StringEntity(input, ContentType.APPLICATION_JSON));
            try {
                CloseableHttpResponse response = client.execute(request);
                JSONObject jsonObject = new JSONObject(EntityUtils.toString(response.getEntity()));
                int numResults = jsonObject.getInt("size");
                JSONArray resArray = jsonObject.getJSONArray("result");
                ArrayList<LdbcShortQuery7MessageRepliesResult> results = new ArrayList<>();
                for (int i = 0; i < numResults; i ++) {
                    JSONArray res = resArray.getJSONArray(i);
                    long commentId = res.getLong(0);
                    String commentContent = res.getString(1);
                    long commentCreationDate = res.getLong(2);
                    long replyAuthorId = res.getLong(3);
                    String replyAuthorFirstName = res.getString(4);
                    String replyAuthorLastName = res.getString(5);
                    boolean replyAuthorKnowsOriginalMessageAuthor = res.getBoolean(6);
                    LdbcShortQuery7MessageRepliesResult result = new LdbcShortQuery7MessageRepliesResult(
                        commentId, commentContent, commentCreationDate,
                        replyAuthorId, replyAuthorFirstName, replyAuthorLastName,
                        replyAuthorKnowsOriginalMessageAuthor
                    );
                    results.add(result);
                }
                resultReporter.report(0, results, operation);
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
                CloseableHttpClient client = dbConnectionState.getClient();
                String endpoint = dbConnectionState.getEndpoint();
                String token = dbConnectionState.getToken();
                HttpPost request = new HttpPost("http://" + endpoint + "/cypher");
                request.setHeader("Accept", "application/json");
                request.setHeader("Authorization", "Bearer "+token);
                String input = String.format("{\"graph\":\"default\", \"script\":\"MATCH (c:Place {id:%d}) CREATE (p:Person {id: %d, firstName: '%s', lastName: $lastName1, gender: '%s', birthday: %d, creationDate: %d, locationIP: '%s', browserUsed: '%s', place:%d, speaks: '%s', email: '%s'})-[:personIsLocatedIn]->(c) WITH p UNWIND %s AS tagId MATCH (t:Tag {id: tagId}) CREATE (p)-[:hasInterest ]->(t)\", \"parameters\":{\"$lastName1\":\"%s\"}}",
                        operation.getCityId(), operation.getPersonId(), operation.getPersonFirstName(), operation.getGender(), operation.getBirthday().getTime(),
                        operation.getCreationDate().getTime(), operation.getLocationIp(), operation.getBrowserUsed(), operation.getCityId(),
                        String.join(";", operation.getLanguages()), String.join(";", operation.getEmails()), operation.getTagIds(), operation.getPersonLastName());
                request.setEntity(new StringEntity(input, ContentType.APPLICATION_JSON));
                CloseableHttpResponse response = client.execute(request);
                String res = EntityUtils.toString(response.getEntity());
//                dbConnectionState.logUpdate(String.format("[\"LdbcUpdate1AddPerson\",%d,%s,%s,%s,%d,%d,%s,%s,%d,%s,%s,%s]|%s\n",
//                        operation.personId(), operation.personFirstName(), operation.personLastName(), operation.gender(), operation.birthday().getTime(),
//                        operation.creationDate().getTime(), operation.locationIp(), operation.browserUsed(), operation.cityId(),String.join(";", operation.languages()),
//                        String.join(";", operation.emails()), operation.tagIds(), res));
                short resultCode = (short)response.getStatusLine().getStatusCode();
                
                for (LdbcUpdate1AddPerson.Organization org : operation.getStudyAt()) {
                    String loop = String.format("{\"graph\":\"default\", \"script\":\"MATCH (p:Person {id: %d}),(u:Organisation {id: %d}) CREATE (p)-[:studyAt {classYear: %d}]->(u)\"}",
                            operation.getPersonId(), org.getOrganizationId(), org.getYear());
                    request.setEntity(new StringEntity(loop, ContentType.APPLICATION_JSON));
                    CloseableHttpResponse resp = client.execute(request);
                    String r = EntityUtils.toString(resp.getEntity());
//                    dbConnectionState.logUpdate(String.format("[\"LdbcUpdate1AddPerson\",%d,%s,%s,%s,%d,%d,%s,%s,%d,%s,%s,%s studyAt[year:%d,id:%d]]|%s\n",
//                            operation.personId(), operation.personFirstName(), operation.personLastName(), operation.gender(), operation.birthday().getTime(),
//                            operation.creationDate().getTime(), operation.locationIp(), operation.browserUsed(), operation.cityId(),String.join(";", operation.languages()),
//                            String.join(";", operation.emails()), operation.tagIds(), org.year(), org.organizationId(), r));
                }
                
                for (LdbcUpdate1AddPerson.Organization org : operation.getWorkAt()) {
                    String loop = String.format("{\"graph\":\"default\", \"script\":\"MATCH (p:Person {id: %d}),(comp:Organisation {id: %d}) CREATE (p)-[:workAt {workFrom: %d}]->(comp)\"}",
                            operation.getPersonId(), org.getOrganizationId(), org.getYear());
                    request.setEntity(new StringEntity(loop, ContentType.APPLICATION_JSON));
                    CloseableHttpResponse resp = client.execute(request);
                    String r = EntityUtils.toString(resp.getEntity());
//                    dbConnectionState.logUpdate(String.format("[\"LdbcUpdate1AddPerson\",%d,%s,%s,%s,%d,%d,%s,%s,%d,%s,%s,%s workAt[year:%d,id:%d]]|%s\n",
//                            operation.personId(), operation.personFirstName(), operation.personLastName(), operation.gender(), operation.birthday().getTime(),
//                            operation.creationDate().getTime(), operation.locationIp(), operation.browserUsed(), operation.cityId(),String.join(";", operation.languages()),
//                            String.join(";", operation.emails()), operation.tagIds(), org.year(), org.organizationId(), r));
                }
                
                resultReporter.report(resultCode, LdbcNoResult.INSTANCE, operation);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcUpdate2AddPostLikeHandler implements OperationHandler<LdbcUpdate2AddPostLike, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate2AddPostLike operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            CloseableHttpClient client = dbConnectionState.getClient();
            String endpoint = dbConnectionState.getEndpoint();
            String token = dbConnectionState.getToken();
            HttpPost request = new HttpPost("http://" + endpoint + "/cypher");
            request.setHeader("Accept", "application/json");
            request.setHeader("Authorization", "Bearer "+token);
            String input = String.format("{\"graph\":\"default\", \"script\":\"MATCH (person:Person {id:%d}),(post:Post {id:%d}) CREATE (person)-[:likes {creationDate:%d}]->(post)\"}", operation.getPersonId(), operation.getPostId(), operation.getCreationDate().getTime());
            request.setEntity(new StringEntity(input, ContentType.APPLICATION_JSON));
            try {
                CloseableHttpResponse response = client.execute(request);
                String res = EntityUtils.toString(response.getEntity());
//                dbConnectionState.logUpdate(String.format("[\"LdbcUpdate2AddPostLike\",%d,%d,%d]|%s\n",
//                        operation.personId(), operation.postId(), operation.creationDate().getTime(), res));
                short resultCode = (short)response.getStatusLine().getStatusCode();
                resultReporter.report(resultCode, LdbcNoResult.INSTANCE, operation);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcUpdate3AddCommentLikeHandler implements OperationHandler<LdbcUpdate3AddCommentLike, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate3AddCommentLike operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            CloseableHttpClient client = dbConnectionState.getClient();
            String endpoint = dbConnectionState.getEndpoint();
            String token = dbConnectionState.getToken();
            HttpPost request = new HttpPost("http://" + endpoint + "/cypher");
            request.setHeader("Accept", "application/json");
            request.setHeader("Authorization", "Bearer "+token);
            String input = String.format("{\"graph\":\"default\", \"script\":\"MATCH (person:Person {id:%d}),(comment:Comment {id:%d}) CREATE (person)-[:likes {creationDate:%d}]->(comment)\"}", operation.getPersonId(), operation.getCommentId(), operation.getCreationDate().getTime());
            request.setEntity(new StringEntity(input, ContentType.APPLICATION_JSON));
            try {
                CloseableHttpResponse response = client.execute(request);
                String res = EntityUtils.toString(response.getEntity());
//                dbConnectionState.logUpdate(String.format("[\"LdbcUpdate3AddCommentLike\",%d,%d,%d]|%s\n",
//                        operation.personId(), operation.commentId(), operation.creationDate().getTime(), res));
                short resultCode = (short)response.getStatusLine().getStatusCode();
                resultReporter.report(resultCode, LdbcNoResult.INSTANCE, operation);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcUpdate4AddForumHandler implements OperationHandler<LdbcUpdate4AddForum, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate4AddForum operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            CloseableHttpClient client = dbConnectionState.getClient();
            String endpoint = dbConnectionState.getEndpoint();
            String token = dbConnectionState.getToken();
            HttpPost request = new HttpPost("http://" + endpoint + "/cypher");
            request.setHeader("Accept", "application/json");
            request.setHeader("Authorization", "Bearer "+token);
            String input = String.format("{\"graph\":\"default\", \"script\":\"MATCH (p:Person {id: %d}) CREATE (f:Forum {id: %d, title: $title1, creationDate: %d, moderator:%d})-[:hasModerator]->(p) WITH f UNWIND %s AS tagId MATCH (t:Tag {id: tagId}) CREATE (f)-[:forumHasTag]->(t)\", \"parameters\":{\"$title1\":\"%s\"}}", operation.getModeratorPersonId(), operation.getForumId(), operation.getCreationDate().getTime(), operation.getModeratorPersonId(), operation.getTagIds(), operation.getForumTitle());
            request.setEntity(new StringEntity(input, ContentType.APPLICATION_JSON));
            try {
                CloseableHttpResponse response = client.execute(request);
                String res = EntityUtils.toString(response.getEntity());
//                dbConnectionState.logUpdate(String.format("[\"LdbcUpdate4AddForum\",%d,%s,%d,%d,%s]|%s\n",
//                        operation.forumId(), operation.forumTitle(), operation.creationDate().getTime(), operation.moderatorPersonId(), operation.tagIds(), res));
                short resultCode = (short)response.getStatusLine().getStatusCode();
                resultReporter.report(resultCode, LdbcNoResult.INSTANCE, operation);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcUpdate5AddForumMembershipHandler implements OperationHandler<LdbcUpdate5AddForumMembership, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate5AddForumMembership operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            CloseableHttpClient client = dbConnectionState.getClient();
            String endpoint = dbConnectionState.getEndpoint();
            String token = dbConnectionState.getToken();
            HttpPost request = new HttpPost("http://" + endpoint + "/cypher");
            request.setHeader("Accept", "application/json");
            request.setHeader("Authorization", "Bearer "+token);
            String input = String.format("{\"graph\":\"default\", \"script\":\"MATCH (f:Forum {id:%d}), (p:Person {id:%d}) CREATE (f)-[:hasMember {joinDate:%d, numPosts:-1}]->(p)\"}", operation.getForumId(), operation.getPersonId(), operation.getJoinDate().getTime());
            request.setEntity(new StringEntity(input, ContentType.APPLICATION_JSON));
            try {
                CloseableHttpResponse response = client.execute(request);
                String res = EntityUtils.toString(response.getEntity());
//                dbConnectionState.logUpdate(String.format("[\"LdbcUpdate5AddForumMembership\",%d,%d,%d]|%s\n",
//                        operation.forumId(), operation.personId(), operation.joinDate().getTime(), res));
                short resultCode = (short)response.getStatusLine().getStatusCode();
                resultReporter.report(resultCode, LdbcNoResult.INSTANCE, operation);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcUpdate6AddPostHandler implements OperationHandler<LdbcUpdate6AddPost, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate6AddPost operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            CloseableHttpClient client = dbConnectionState.getClient();
            String endpoint = dbConnectionState.getEndpoint();
            String token = dbConnectionState.getToken();
            HttpPost request = new HttpPost("http://" + endpoint + "/cypher");
            request.setHeader("Accept", "application/json");
            request.setHeader("Authorization", "Bearer "+token);            
            String input = "";
            if (!operation.getContent().isEmpty()) {
                input = String.format("{\"graph\":\"default\", \"script\":\"MATCH (author:Person {id: %d}), (country:Place {id: %d}), (forum:Forum {id: %d}) CREATE (author)<-[:postHasCreator {creationDate:%d}]-(p:Post {id: %d, creationDate: %d, locationIP: '%s', browserUsed: '%s', content: $content1, length: %d, creator:%d, container:%d, place:%d})<-[:containerOf]-(forum), (p)-[:postIsLocatedIn {creationDate:%d}]->(country) WITH p UNWIND %s AS tagId MATCH (t:Tag {id: tagId}) CREATE (p)-[:postHasTag]->(t)\", \"parameters\":{\"$content1\":\"%s\"}}",
                    operation.getAuthorPersonId(), operation.getCountryId(), operation.getForumId(), operation.getCreationDate().getTime(), operation.getPostId(),
                    operation.getCreationDate().getTime(), operation.getLocationIp(), operation.getBrowserUsed(), operation.getLength(), operation.getAuthorPersonId(),
                    operation.getForumId(), operation.getCountryId(), operation.getCreationDate().getTime(), operation.getTagIds(), operation.getContent());
            }
            else if (!operation.getImageFile().isEmpty()) {
                input = String.format("{\"graph\":\"default\", \"script\":\"MATCH (author:Person {id: %d}), (country:Place {id: %d}), (forum:Forum {id: %d}) CREATE (author)<-[:postHasCreator {creationDate:%d}]-(p:Post {id: %d, creationDate: %d, locationIP: '%s', browserUsed: '%s', imageFile: $imageFile1, length: %d, creator:%d, container:%d, place:%d})<-[:containerOf]-(forum), (p)-[:postIsLocatedIn {creationDate:%d}]->(country) WITH p UNWIND %s AS tagId MATCH (t:Tag {id: tagId}) CREATE (p)-[:postHasTag]->(t)\", \"parameters\":{\"$imageFile1\":\"%s\"}}",
                    operation.getAuthorPersonId(), operation.getCountryId(), operation.getForumId(), operation.getCreationDate().getTime(), operation.getPostId(),
                    operation.getCreationDate().getTime(), operation.getLocationIp(), operation.getBrowserUsed(), operation.getLength(), operation.getAuthorPersonId(),
                    operation.getForumId(), operation.getCountryId(), operation.getCreationDate().getTime(), operation.getTagIds(), operation.getImageFile());
            }
            request.setEntity(new StringEntity(input, ContentType.APPLICATION_JSON));
            try {
                CloseableHttpResponse response = client.execute(request);
                String res = EntityUtils.toString(response.getEntity());
//                dbConnectionState.logUpdate(String.format("[\"LdbcUpdate6AddPost\",%d,%s,%d,%s,%s,%s,%d,%d,%d,%d,%s]|%s\n",
//                        operation.postId(), operation.imageFile(), operation.creationDate().getTime(), operation.locationIp(), operation.browserUsed(),
//                        operation.content(), operation.length(), operation.authorPersonId(), operation.forumId(), operation.countryId(),
//                        operation.tagIds(), res));
                short resultCode = (short)response.getStatusLine().getStatusCode();
                resultReporter.report(resultCode, LdbcNoResult.INSTANCE, operation);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcUpdate7AddCommentHandler implements OperationHandler<LdbcUpdate7AddComment, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate7AddComment operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            CloseableHttpClient client = dbConnectionState.getClient();
            String endpoint = dbConnectionState.getEndpoint();
            String token = dbConnectionState.getToken();
            HttpPost request = new HttpPost("http://" + endpoint + "/cypher");
            request.setHeader("Accept", "application/json");
            request.setHeader("Authorization", "Bearer "+token);
            long msgId = operation.getReplyToPostId() + operation.getReplyToCommentId() + 1;
            String input = String.format("{\"graph\":\"default\", \"script\":\"MATCH (author:Person {id: %d}), (country:Place {id: %d}), (message {id: %d}) CREATE (author)<-[:commentHasCreator {creationDate:%d}]-(c:Comment {id: %d, creationDate: %d, locationIP: '%s', browserUsed: '%s', content: $content1, length: %d, creator:%d,place:%d})-[:replyOf {creationDate:%d}]->(message), (c)-[:commentIsLocatedIn {creationDate:%d}]->(country) WITH c UNWIND %s AS tagId MATCH (t:Tag {id: tagId}) CREATE (c)-[:commentHasTag]->(t)\", \"parameters\":{\"$content1\":\"%s\"}}",
                    operation.getAuthorPersonId(), operation.getCountryId(), msgId, operation.getCreationDate().getTime(), operation.getCommentId(),
                    operation.getCreationDate().getTime(), operation.getLocationIp(), operation.getBrowserUsed(), operation.getLength(), operation.getAuthorPersonId(),
                    operation.getCountryId(), operation.getCreationDate().getTime(), operation.getCreationDate().getTime(), operation.getTagIds(), operation.getContent());
            request.setEntity(new StringEntity(input, ContentType.APPLICATION_JSON));
            try {
                CloseableHttpResponse response = client.execute(request);
                String res = EntityUtils.toString(response.getEntity());
//                dbConnectionState.logUpdate(String.format("[\"LdbcUpdate7AddComment\",%d,%d,%s,%s,%s,%d,%d,%d,%d,%d,%s]|%s\n",
//                        operation.commentId(), operation.creationDate().getTime(), operation.locationIp(), operation.browserUsed(), operation.content(),
//                        operation.length(), operation.authorPersonId(), operation.countryId(), operation.replyToPostId(), operation.replyToCommentId(),
//                        operation.tagIds(), res));
                short resultCode = (short)response.getStatusLine().getStatusCode();
                resultReporter.report(resultCode, LdbcNoResult.INSTANCE, operation);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcUpdate8AddFriendshipHandler implements OperationHandler<LdbcUpdate8AddFriendship, LightGraphCypherDbConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate8AddFriendship operation, LightGraphCypherDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            CloseableHttpClient client = dbConnectionState.getClient();
            String endpoint = dbConnectionState.getEndpoint();
            String token = dbConnectionState.getToken();
            HttpPost request = new HttpPost("http://" + endpoint + "/cypher");
            request.setHeader("Accept", "application/json");
            request.setHeader("Authorization", "Bearer "+token);
            String input = String.format("{\"graph\":\"default\", \"script\":\"MATCH (p1:Person {id:%d}), (p2:Person {id:%d}) WITH p1,p2 OPTIONAL MATCH (p1)<-[:commentHasCreator]-(c)-[:replyOf]->(:Post)-[:postHasCreator]->(p2) WITH p1,p2,count(*) AS replyOnPost OPTIONAL MATCH (p1)<-[:commentHasCreator]-(c)-[:replyOf]->(:Comment)-[:commentHasCreator]->(p2) WITH p1,p2,replyOnPost,count(*) AS replyOnComment CREATE (p1)-[:knows {creationDate:%d, weight:replyOnPost*1.0+replyOnComment*0.5}]->(p2)\"}", operation.getPerson1Id(), operation.getPerson2Id(), operation.getCreationDate().getTime());
            request.setEntity(new StringEntity(input, ContentType.APPLICATION_JSON));
            try {
                CloseableHttpResponse response = client.execute(request);
                String res = EntityUtils.toString(response.getEntity());
//                dbConnectionState.logUpdate(String.format("[\"LdbcUpdate8AddFriendship\",%d,%d,%d]|%s\n",
//                        operation.person1Id(), operation.person2Id(), operation.creationDate().getTime(), res));
                short resultCode = (short)response.getStatusLine().getStatusCode();
                resultReporter.report(resultCode, LdbcNoResult.INSTANCE, operation);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
