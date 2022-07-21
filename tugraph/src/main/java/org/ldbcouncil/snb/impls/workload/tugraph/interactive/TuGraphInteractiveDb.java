package org.ldbcouncil.snb.impls.workload.tugraph.interactive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.ldbcouncil.snb.impls.workload.tugraph.CustomDataInputStream;
import org.ldbcouncil.snb.impls.workload.tugraph.CustomDataOutputStream;
import org.ldbcouncil.snb.impls.workload.tugraph.TuGraphClient;
import org.ldbcouncil.snb.impls.workload.tugraph.TuGraphDbConnectionState;
import org.apache.commons.codec.binary.Base64;
import com.google.common.collect.Lists;

import org.ldbcouncil.snb.driver.Db;
import org.ldbcouncil.snb.driver.DbConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.control.LoggingService;
import org.ldbcouncil.snb.driver.workloads.interactive.*;

public class TuGraphInteractiveDb extends Db {
	
	TuGraphDbConnectionState dcs;

	@Override
	protected void onInit(Map<String, String> properties, LoggingService loggingService) throws DbException {
		try {
			dcs = new TuGraphDbConnectionState(properties);
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
	
	public static class LdbcQuery1Handler implements OperationHandler<LdbcQuery1, TuGraphDbConnectionState> {

		@Override
		public void executeOperation(LdbcQuery1 operation, TuGraphDbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {
			try {
				CustomDataOutputStream cdos = new CustomDataOutputStream();
				cdos.writeInt64(operation.getPersonIdQ1());
				cdos.writeString(operation.getFirstName());
				String input = Base64.encodeBase64String(cdos.toByteArray());
				TuGraphClient client = dbConnectionState.popClient();
				String op = "CALL_CPP_PLUGIN";
				String res = "interactive_complex_read_1";
				CustomDataInputStream cdis = client.call(op,  res, input);
				short numResults = cdis.readInt16();
				ArrayList<LdbcQuery1Result> results = new ArrayList<>();
				for (int i = 0; i < numResults; i ++) {
					long friendId = cdis.readInt64();
					String friendLastName = cdis.readString();
					int distanceFromPerson = cdis.readInt32();
					long friendBirthday = cdis.readInt64();
					long friendCreationDate = cdis.readInt64();
					String friendGender = cdis.readString();
					String friendBrowserUsed = cdis.readString();
					String friendLocationIp = cdis.readString();
					ArrayList<String> friendEmails = Lists.newArrayList(cdis.readString().split(";"));
					ArrayList<String> friendLanguages = Lists.newArrayList(cdis.readString().split(";"));
					String friendCityName = cdis.readString();
					short numStudyExp = cdis.readInt16();
					ArrayList<LdbcQuery1Result.Organization> friendUniversities = new ArrayList<>();
					for (int j = 0; j < numStudyExp; j ++) {
						LdbcQuery1Result.Organization item = new LdbcQuery1Result.Organization(
								cdis.readString(),
								cdis.readInt32(),
								cdis.readString());
						friendUniversities.add(item);
					}
					short numWorkExp = cdis.readInt16();
					ArrayList<LdbcQuery1Result.Organization> friendCompanies = new ArrayList<>();
					for (int j = 0; j < numWorkExp; j ++) {
						LdbcQuery1Result.Organization item = new LdbcQuery1Result.Organization(
								cdis.readString(),
								cdis.readInt32(),
								cdis.readString());
						friendCompanies.add(item);
					}
					LdbcQuery1Result result = new LdbcQuery1Result(
						friendId, friendLastName, distanceFromPerson, friendBirthday, friendCreationDate, friendGender, friendBrowserUsed, friendLocationIp,
						friendEmails, friendLanguages, friendCityName, friendUniversities, friendCompanies
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

	public static class LdbcQuery2Handler implements OperationHandler<LdbcQuery2, TuGraphDbConnectionState> {

		@Override
		public void executeOperation(LdbcQuery2 operation, TuGraphDbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {
			try {
				CustomDataOutputStream cdos = new CustomDataOutputStream();
				cdos.writeInt64(operation.getPersonIdQ2());
				cdos.writeInt64(operation.getMaxDate().getTime());
				String input = Base64.encodeBase64String(cdos.toByteArray());
				TuGraphClient client = dbConnectionState.popClient();
				String op = "CALL_CPP_PLUGIN";
				String res = "interactive_complex_read_2";
				CustomDataInputStream cdis = client.call(op,  res, input);
				short numResults = cdis.readInt16();
				ArrayList<LdbcQuery2Result> results = new ArrayList<>();
				for (int i = 0; i < numResults; i ++) {
					long personId = cdis.readInt64();
					String personFirstName = cdis.readString();
					String personLastName = cdis.readString();
					long messageId = cdis.readInt64();
					String messageContent = cdis.readString();
					long messageCreationDate = cdis.readInt64();
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

	public static class LdbcQuery3Handler implements OperationHandler<LdbcQuery3, TuGraphDbConnectionState> {

		@Override
		public void executeOperation(LdbcQuery3 operation, TuGraphDbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {
			try {
				CustomDataOutputStream cdos = new CustomDataOutputStream();
				cdos.writeInt64(operation.getPersonIdQ3());
				cdos.writeString(operation.getCountryXName());
				cdos.writeString(operation.getCountryYName());
				cdos.writeInt64(operation.getStartDate().getTime());
				cdos.writeInt32(operation.getDurationDays());
				String input = Base64.encodeBase64String(cdos.toByteArray());
				TuGraphClient client = dbConnectionState.popClient();
				String op = "CALL_CPP_PLUGIN";
				String res = "interactive_complex_read_3";
				CustomDataInputStream cdis = client.call(op,  res, input);
				short numResults = cdis.readInt16();
				ArrayList<LdbcQuery3Result> results = new ArrayList<>();
				for (int i = 0; i < numResults; i ++) {
					long personId = cdis.readInt64();
					String personFirstName = cdis.readString();
					String personLastName = cdis.readString();
					int xCount = cdis.readInt32();
					int yCount = cdis.readInt32();
					int count = cdis.readInt32();
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

	public static class LdbcQuery4Handler implements OperationHandler<LdbcQuery4, TuGraphDbConnectionState> {

		@Override
		public void executeOperation(LdbcQuery4 operation, TuGraphDbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {
			try {
				CustomDataOutputStream cdos = new CustomDataOutputStream();
				cdos.writeInt64(operation.getPersonIdQ4());
				cdos.writeInt64(operation.getStartDate().getTime());
				cdos.writeInt32(operation.getDurationDays());
				String input = Base64.encodeBase64String(cdos.toByteArray());
				TuGraphClient client = dbConnectionState.popClient();
				String op = "CALL_CPP_PLUGIN";
				String res = "interactive_complex_read_4";
				CustomDataInputStream cdis = client.call(op,  res, input);
				short numResults = cdis.readInt16();
				ArrayList<LdbcQuery4Result> results = new ArrayList<>();
				for (int i = 0; i < numResults; i ++) {
					String tagName = cdis.readString();
					int postCount = cdis.readInt32();
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

	public static class LdbcQuery5Handler implements OperationHandler<LdbcQuery5, TuGraphDbConnectionState> {

		@Override
		public void executeOperation(LdbcQuery5 operation, TuGraphDbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {
			try {
				CustomDataOutputStream cdos = new CustomDataOutputStream();
				cdos.writeInt64(operation.getPersonIdQ5());
				cdos.writeInt64(operation.getMinDate().getTime());
				String input = Base64.encodeBase64String(cdos.toByteArray());
				TuGraphClient client = dbConnectionState.popClient();
				String op = "CALL_CPP_PLUGIN";
				String res = "interactive_complex_read_5";
				CustomDataInputStream cdis = client.call(op,  res, input);
				short numResults = cdis.readInt16();
				ArrayList<LdbcQuery5Result> results = new ArrayList<>();
				for (int i = 0; i < numResults; i ++) {
					String forumTitle = cdis.readString();
					int postCount = cdis.readInt32();
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

	public static class LdbcQuery6Handler implements OperationHandler<LdbcQuery6, TuGraphDbConnectionState> {

		@Override
		public void executeOperation(LdbcQuery6 operation, TuGraphDbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {
			try {
				CustomDataOutputStream cdos = new CustomDataOutputStream();
				cdos.writeInt64(operation.getPersonIdQ6());
				cdos.writeString(operation.getTagName());
				String input = Base64.encodeBase64String(cdos.toByteArray());
				TuGraphClient client = dbConnectionState.popClient();
				String op = "CALL_CPP_PLUGIN";
				String res = "interactive_complex_read_6";
				CustomDataInputStream cdis = client.call(op,  res, input);
				short numResults = cdis.readInt16();
				ArrayList<LdbcQuery6Result> results = new ArrayList<>();
				for (int i = 0; i < numResults; i ++) {
					String tagName = cdis.readString();
					int postCount = cdis.readInt32();
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


	public static class LdbcQuery7Handler implements OperationHandler<LdbcQuery7, TuGraphDbConnectionState> {

		@Override
		public void executeOperation(LdbcQuery7 operation, TuGraphDbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {
			try {
				CustomDataOutputStream cdos = new CustomDataOutputStream();
				cdos.writeInt64(operation.getPersonIdQ7());
				String input = Base64.encodeBase64String(cdos.toByteArray());
				TuGraphClient client = dbConnectionState.popClient();
				String op = "CALL_CPP_PLUGIN";
				String res = "interactive_complex_read_7";
				CustomDataInputStream cdis = client.call(op,  res, input);
				short numResults = cdis.readInt16();
				ArrayList<LdbcQuery7Result> results = new ArrayList<>();
				for (int i = 0; i < numResults; i ++) {
					long personId = cdis.readInt64();
					String personFirstName = cdis.readString();
					String personLastName = cdis.readString();
					long likeCreationDate = cdis.readInt64();
					long commentOrPostId = cdis.readInt64();
					String commentOrPostContent = cdis.readString();
					int minutesLatency = cdis.readInt32();
					boolean isNew = cdis.readBoolean();
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

	public static class LdbcQuery8Handler implements OperationHandler<LdbcQuery8, TuGraphDbConnectionState> {

		@Override
		public void executeOperation(LdbcQuery8 operation, TuGraphDbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {
			try {
				CustomDataOutputStream cdos = new CustomDataOutputStream();
				cdos.writeInt64(operation.getPersonIdQ8());
				String input = Base64.encodeBase64String(cdos.toByteArray());
				TuGraphClient client = dbConnectionState.popClient();
				String op = "CALL_CPP_PLUGIN";
				String res = "interactive_complex_read_8";
				CustomDataInputStream cdis = client.call(op,  res, input);
				short numResults = cdis.readInt16();
				ArrayList<LdbcQuery8Result> results = new ArrayList<>();
				for (int i = 0; i < numResults; i ++) {
					long personId = cdis.readInt64();
					String personFirstName = cdis.readString();
					String personLastName = cdis.readString();
					long commentCreationDate = cdis.readInt64();
					long commentId = cdis.readInt64();
					String commentContent = cdis.readString();
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

	public static class LdbcQuery9Handler implements OperationHandler<LdbcQuery9, TuGraphDbConnectionState> {

		@Override
		public void executeOperation(LdbcQuery9 operation, TuGraphDbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {
			try {
				CustomDataOutputStream cdos = new CustomDataOutputStream();
				cdos.writeInt64(operation.getPersonIdQ9());
				cdos.writeInt64(operation.getMaxDate().getTime());
				String input = Base64.encodeBase64String(cdos.toByteArray());
				TuGraphClient client = dbConnectionState.popClient();
				String op = "CALL_CPP_PLUGIN";
				String res = "interactive_complex_read_9";
				CustomDataInputStream cdis = client.call(op,  res, input);
				short numResults = cdis.readInt16();
				ArrayList<LdbcQuery9Result> results = new ArrayList<>();
				for (int i = 0; i < numResults; i ++) {
					long personId = cdis.readInt64();
					String personFirstName = cdis.readString();
					String personLastName = cdis.readString();
					long messageId = cdis.readInt64();
					String messageContent = cdis.readString();
					long messageCreationDate = cdis.readInt64();
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

	public static class LdbcQuery10Handler implements OperationHandler<LdbcQuery10, TuGraphDbConnectionState> {

		@Override
		public void executeOperation(LdbcQuery10 operation, TuGraphDbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {
			try {
				CustomDataOutputStream cdos = new CustomDataOutputStream();
				cdos.writeInt64(operation.getPersonIdQ10());
				cdos.writeInt32(operation.getMonth());
				String input = Base64.encodeBase64String(cdos.toByteArray());
				TuGraphClient client = dbConnectionState.popClient();
				String op = "CALL_CPP_PLUGIN";
				String res = "interactive_complex_read_10";
				CustomDataInputStream cdis = client.call(op,  res, input);
				short numResults = cdis.readInt16();
				ArrayList<LdbcQuery10Result> results = new ArrayList<>();
				for (int i = 0; i < numResults; i ++) {
					long personId = cdis.readInt64();
					String personFirstName = cdis.readString();
					String personLastName = cdis.readString();
					int commonInterestScore = cdis.readInt32();
					String personGender = cdis.readString();
					String personCityName = cdis.readString();
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

	public static class LdbcQuery11Handler implements OperationHandler<LdbcQuery11, TuGraphDbConnectionState> {

		@Override
		public void executeOperation(LdbcQuery11 operation, TuGraphDbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {
			try {
				CustomDataOutputStream cdos = new CustomDataOutputStream();
				cdos.writeInt64(operation.getPersonIdQ11());
				cdos.writeString(operation.getCountryName());
				cdos.writeInt32(operation.getWorkFromYear());
				String input = Base64.encodeBase64String(cdos.toByteArray());
				TuGraphClient client = dbConnectionState.popClient();
				String op = "CALL_CPP_PLUGIN";
				String res = "interactive_complex_read_11";
				CustomDataInputStream cdis = client.call(op,  res, input);
				short numResults = cdis.readInt16();
				ArrayList<LdbcQuery11Result> results = new ArrayList<>();
				for (int i = 0; i < numResults; i ++) {
					long personId = cdis.readInt64();
					String personFirstName = cdis.readString();
					String personLastName = cdis.readString();
					String organisationName = cdis.readString();
					int organisationWorkFromYear = cdis.readInt32();
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

	public static class LdbcQuery12Handler implements OperationHandler<LdbcQuery12, TuGraphDbConnectionState> {

		@Override
		public void executeOperation(LdbcQuery12 operation, TuGraphDbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {
			try {
				CustomDataOutputStream cdos = new CustomDataOutputStream();
				cdos.writeInt64(operation.getPersonIdQ12());
				cdos.writeString(operation.getTagClassName());
				String input = Base64.encodeBase64String(cdos.toByteArray());
				TuGraphClient client = dbConnectionState.popClient();
				String op = "CALL_CPP_PLUGIN";
				String res = "interactive_complex_read_12";
				CustomDataInputStream cdis = client.call(op,  res, input);
				short numResults = cdis.readInt16();
				ArrayList<LdbcQuery12Result> results = new ArrayList<>();
				for (int i = 0; i < numResults; i ++) {
					long personId = cdis.readInt64();
					String personFirstName = cdis.readString();
					String personLastName = cdis.readString();
					short numTags = cdis.readInt16();
					ArrayList<String> tagNames = new ArrayList<String>();
					for (int j = 0; j < numTags; j ++) {
						tagNames.add(cdis.readString());
					}
					int replyCount = cdis.readInt32();
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


	public static class LdbcQuery13Handler implements OperationHandler<LdbcQuery13, TuGraphDbConnectionState> {

		@Override
		public void executeOperation(LdbcQuery13 operation, TuGraphDbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {
			try {
				CustomDataOutputStream cdos = new CustomDataOutputStream();
				cdos.writeInt64(operation.getPerson1IdQ13StartNode());
				cdos.writeInt64(operation.getPerson2IdQ13EndNode());
				String input = Base64.encodeBase64String(cdos.toByteArray());
				TuGraphClient client = dbConnectionState.popClient();
				String op = "CALL_CPP_PLUGIN";
				String res = "interactive_complex_read_13";
				CustomDataInputStream cdis = client.call(op,  res, input);
				int shortestPathLength = cdis.readInt32();
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

	public static class LdbcQuery14Handler implements OperationHandler<LdbcQuery14, TuGraphDbConnectionState> {

		@Override
		public void executeOperation(LdbcQuery14 operation, TuGraphDbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {
			try {
				CustomDataOutputStream cdos = new CustomDataOutputStream();
				cdos.writeInt64(operation.getPerson1IdQ14StartNode());
				cdos.writeInt64(operation.getPerson2IdQ14EndNode());
				String input = Base64.encodeBase64String(cdos.toByteArray());
				TuGraphClient client = dbConnectionState.popClient();
				String op = "CALL_CPP_PLUGIN";
				String res = "interactive_complex_read_14";
				CustomDataInputStream cdis = client.call(op,  res, input);
				int numResults = cdis.readInt32();
				int pathLength = cdis.readInt32();
				ArrayList<LdbcQuery14Result> results = new ArrayList<>();
				for (int i = 0; i < numResults; i ++) {
					ArrayList<Long> personIdsInPath = new ArrayList<>();
					for (int j = 0; j <= pathLength; j ++) {
						personIdsInPath.add(cdis.readInt64());
					}
					double pathWeight = cdis.readDouble();
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

    public static class LdbcShortQuery1PersonProfileHandler implements OperationHandler<LdbcShortQuery1PersonProfile, TuGraphDbConnectionState> {

		@Override
		public void executeOperation(LdbcShortQuery1PersonProfile operation,
									 TuGraphDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
			try {
				CustomDataOutputStream cdos = new CustomDataOutputStream();
				cdos.writeInt64(operation.getPersonIdSQ1());
				String input = Base64.encodeBase64String(cdos.toByteArray());
				TuGraphClient client = dbConnectionState.popClient();
				String op = "CALL_CPP_PLUGIN";
				String res = "interactive_short_read_1";
				CustomDataInputStream cdis = client.call(op,  res, input);
				String firstName = cdis.readString();
				String lastName = cdis.readString();
				long birthday = cdis.readInt64();
				String locationIP = cdis.readString();
				String browserUsed = cdis.readString();
				long cityId = cdis.readInt64();
				String gender = cdis.readString();
				long creationDate = cdis.readInt64();
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

    public static class LdbcShortQuery2PersonPostsHandler implements OperationHandler<LdbcShortQuery2PersonPosts, TuGraphDbConnectionState> {

		@Override
		public void executeOperation(LdbcShortQuery2PersonPosts operation,
									 TuGraphDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
			try {
				CustomDataOutputStream cdos = new CustomDataOutputStream();
				cdos.writeInt64(operation.getPersonIdSQ2());
				String input = Base64.encodeBase64String(cdos.toByteArray());
				TuGraphClient client = dbConnectionState.popClient();
				String op = "CALL_CPP_PLUGIN";
				String res = "interactive_short_read_2";
				CustomDataInputStream cdis = client.call(op,  res, input);
				ArrayList<LdbcShortQuery2PersonPostsResult> results = new ArrayList<>();
				short numResults = cdis.readInt16();
				for (int i = 0; i < numResults; i ++) {
					long messageId = cdis.readInt64();
					String messageContent = cdis.readString();
					long messageCreationDate = cdis.readInt64();
					long originalPostId = cdis.readInt64();
					long originalPostAuthorId = cdis.readInt64();
					String originalPostAuthorFirstName = cdis.readString();
					String originalPostAuthorLastName = cdis.readString();
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

    public static class LdbcShortQuery3PersonFriendsHandler implements OperationHandler<LdbcShortQuery3PersonFriends, TuGraphDbConnectionState> {

		@Override
		public void executeOperation(LdbcShortQuery3PersonFriends operation,
									 TuGraphDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
			try {
				CustomDataOutputStream cdos = new CustomDataOutputStream();
				cdos.writeInt64(operation.getPersonIdSQ3());
				String input = Base64.encodeBase64String(cdos.toByteArray());
				TuGraphClient client = dbConnectionState.popClient();
				String op = "CALL_CPP_PLUGIN";
				String res = "interactive_short_read_3";
				CustomDataInputStream cdis = client.call(op,  res, input);
				ArrayList<LdbcShortQuery3PersonFriendsResult> results = new ArrayList<>();
				short numResults = cdis.readInt16();
				for (int i = 0; i < numResults; i ++) {
					long personId = cdis.readInt64();
					String firstName = cdis.readString();
					String lastName = cdis.readString();
					long friendshipCreationDate = cdis.readInt64();
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

    public static class LdbcShortQuery4MessageContentHandler implements OperationHandler<LdbcShortQuery4MessageContent, TuGraphDbConnectionState> {

		@Override
		public void executeOperation(LdbcShortQuery4MessageContent operation,
									 TuGraphDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
			try {
				CustomDataOutputStream cdos = new CustomDataOutputStream();
				cdos.writeInt64(operation.getMessageIdContent());
				String input = Base64.encodeBase64String(cdos.toByteArray());
				TuGraphClient client = dbConnectionState.popClient();
				String op = "CALL_CPP_PLUGIN";
				String res = "interactive_short_read_4";
				CustomDataInputStream cdis = client.call(op,  res, input);
				long messageCreationDate = cdis.readInt64();
				String messageContent = cdis.readString();
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

    public static class LdbcShortQuery5MessageCreatorHandler implements OperationHandler<LdbcShortQuery5MessageCreator, TuGraphDbConnectionState> {

		@Override
		public void executeOperation(LdbcShortQuery5MessageCreator operation,
									 TuGraphDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
			try {
				CustomDataOutputStream cdos = new CustomDataOutputStream();
				cdos.writeInt64(operation.getMessageIdCreator());
				String input = Base64.encodeBase64String(cdos.toByteArray());
				TuGraphClient client = dbConnectionState.popClient();
				String op = "CALL_CPP_PLUGIN";
				String res = "interactive_short_read_5";
				CustomDataInputStream cdis = client.call(op,  res, input);
				long personId = cdis.readInt64();
				String firstName = cdis.readString();
				String lastName = cdis.readString();
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

    public static class LdbcShortQuery6MessageForumHandler implements OperationHandler<LdbcShortQuery6MessageForum, TuGraphDbConnectionState> {

		@Override
		public void executeOperation(LdbcShortQuery6MessageForum operation,
									 TuGraphDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
			try {
				CustomDataOutputStream cdos = new CustomDataOutputStream();
				cdos.writeInt64(operation.getMessageForumId());
				String input = Base64.encodeBase64String(cdos.toByteArray());
				TuGraphClient client = dbConnectionState.popClient();
				String op = "CALL_CPP_PLUGIN";
				String res = "interactive_short_read_6";
				CustomDataInputStream cdis = client.call(op,  res, input);
				long forumId = cdis.readInt64();
				String forumTitle = cdis.readString();
				long moderatorId = cdis.readInt64();
				String moderatorFirstName = cdis.readString();
				String moderatorLastName = cdis.readString();
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

    public static class LdbcShortQuery7MessageRepliesHandler implements OperationHandler<LdbcShortQuery7MessageReplies, TuGraphDbConnectionState> {

		@Override
		public void executeOperation(LdbcShortQuery7MessageReplies operation,
									 TuGraphDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
			try {
				CustomDataOutputStream cdos = new CustomDataOutputStream();
				cdos.writeInt64(operation.getMessageRepliesId());
				String input = Base64.encodeBase64String(cdos.toByteArray());
				TuGraphClient client = dbConnectionState.popClient();
				String op = "CALL_CPP_PLUGIN";
				String res = "interactive_short_read_7";
				CustomDataInputStream cdis = client.call(op,  res, input);
				ArrayList<LdbcShortQuery7MessageRepliesResult> results = new ArrayList<>();
				short numResults = cdis.readInt16();
				for (int i = 0; i < numResults; i ++) {
					long commentId = cdis.readInt64();
					String commentContent = cdis.readString();
					long commentCreationDate = cdis.readInt64();
					long replyAuthorId = cdis.readInt64();
					String replyAuthorFirstName = cdis.readString();
					String replyAuthorLastName = cdis.readString();
					boolean replyAuthorKnowsOriginalMessageAuthor = cdis.readBoolean();
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

    public static class LdbcUpdate1AddPersonHandler implements OperationHandler<LdbcUpdate1AddPerson, TuGraphDbConnectionState> {

		@Override
		public void executeOperation(LdbcUpdate1AddPerson operation, TuGraphDbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {
			CustomDataOutputStream cdos = new CustomDataOutputStream();
			cdos.writeInt64(operation.getPersonId());
			cdos.writeString(operation.getPersonFirstName());
			cdos.writeString(operation.getPersonLastName());
			cdos.writeString(operation.getGender());
			cdos.writeInt64(operation.getBirthday().getTime());
			cdos.writeInt64(operation.getCreationDate().getTime());
			cdos.writeString(operation.getLocationIp());
			cdos.writeString(operation.getBrowserUsed());
			cdos.writeInt64(operation.getCityId());
			cdos.writeString(String.join(";", operation.getLanguages()));
			cdos.writeString(String.join(";", operation.getEmails()));
			cdos.writeInt16((short)operation.getTagIds().size());
			for (long tagId : operation.getTagIds()) {
				cdos.writeInt64(tagId);
			}
			cdos.writeInt16((short)operation.getStudyAt().size());
			for (LdbcUpdate1AddPerson.Organization org : operation.getStudyAt()) {
				cdos.writeInt64(org.getOrganizationId());
				cdos.writeInt32(org.getYear());
			}
			cdos.writeInt16((short)operation.getWorkAt().size());
			for (LdbcUpdate1AddPerson.Organization org : operation.getWorkAt()) {
				cdos.writeInt64(org.getOrganizationId());
				cdos.writeInt32(org.getYear());
			}
			try {
				TuGraphClient client = dbConnectionState.popClient();
				String op = "CALL_CPP_PLUGIN";
				String res = "interactive_update_1";
				String input = Base64.encodeBase64String(cdos.toByteArray());
				CustomDataInputStream cdis = client.call(op,  res, input);
				short resultCode = cdis.readInt16();
				resultReporter.report(resultCode, LdbcNoResult.INSTANCE, operation);
				dbConnectionState.pushClient(client);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

    }

    public static class LdbcUpdate2AddPostLikeHandler implements OperationHandler<LdbcUpdate2AddPostLike, TuGraphDbConnectionState> {

		@Override
		public void executeOperation(LdbcUpdate2AddPostLike operation, TuGraphDbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {
			CustomDataOutputStream cdos = new CustomDataOutputStream();
			cdos.writeInt64(operation.getPersonId());
			cdos.writeInt64(operation.getPostId());
			cdos.writeInt64(operation.getCreationDate().getTime());
			try {
				TuGraphClient client = dbConnectionState.popClient();
				String op = "CALL_CPP_PLUGIN";
				String res = "interactive_update_2";
				String input = Base64.encodeBase64String(cdos.toByteArray());
				CustomDataInputStream cdis = client.call(op,  res, input);
				short resultCode = cdis.readInt16();
				resultReporter.report(resultCode, LdbcNoResult.INSTANCE, operation);
				dbConnectionState.pushClient(client);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

    }

    public static class LdbcUpdate3AddCommentLikeHandler implements OperationHandler<LdbcUpdate3AddCommentLike, TuGraphDbConnectionState> {

		@Override
		public void executeOperation(LdbcUpdate3AddCommentLike operation, TuGraphDbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {
			CustomDataOutputStream cdos = new CustomDataOutputStream();
			cdos.writeInt64(operation.getPersonId());
			cdos.writeInt64(operation.getCommentId());
			cdos.writeInt64(operation.getCreationDate().getTime());
			try {
				TuGraphClient client = dbConnectionState.popClient();
				String op = "CALL_CPP_PLUGIN";
				String res = "interactive_update_3";
				String input = Base64.encodeBase64String(cdos.toByteArray());
				CustomDataInputStream cdis = client.call(op,  res, input);
				short resultCode = cdis.readInt16();
				resultReporter.report(resultCode, LdbcNoResult.INSTANCE, operation);
				dbConnectionState.pushClient(client);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

    }

    public static class LdbcUpdate4AddForumHandler implements OperationHandler<LdbcUpdate4AddForum, TuGraphDbConnectionState> {

		@Override
		public void executeOperation(LdbcUpdate4AddForum operation, TuGraphDbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {
			CustomDataOutputStream cdos = new CustomDataOutputStream();
			cdos.writeInt64(operation.getForumId());
			cdos.writeString(operation.getForumTitle());
			cdos.writeInt64(operation.getCreationDate().getTime());
			cdos.writeInt64(operation.getModeratorPersonId());
			cdos.writeInt16((short)operation.getTagIds().size());
			for (long tagId : operation.getTagIds()) {
				cdos.writeInt64(tagId);
			}
			try {
				TuGraphClient client = dbConnectionState.popClient();
				String op = "CALL_CPP_PLUGIN";
				String res = "interactive_update_4";
				String input = Base64.encodeBase64String(cdos.toByteArray());
				CustomDataInputStream cdis = client.call(op,  res, input);
				short resultCode = cdis.readInt16();
				resultReporter.report(resultCode, LdbcNoResult.INSTANCE, operation);
				dbConnectionState.pushClient(client);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

    }

    public static class LdbcUpdate5AddForumMembershipHandler implements OperationHandler<LdbcUpdate5AddForumMembership, TuGraphDbConnectionState> {

		@Override
		public void executeOperation(LdbcUpdate5AddForumMembership operation, TuGraphDbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {
			CustomDataOutputStream cdos = new CustomDataOutputStream();
			cdos.writeInt64(operation.getPersonId());
			cdos.writeInt64(operation.getForumId());
			cdos.writeInt64(operation.getJoinDate().getTime());
			try {
				TuGraphClient client = dbConnectionState.popClient();
				String op = "CALL_CPP_PLUGIN";
				String res = "interactive_update_5";
				String input = Base64.encodeBase64String(cdos.toByteArray());
				CustomDataInputStream cdis = client.call(op,  res, input);
				short resultCode = cdis.readInt16();
				resultReporter.report(resultCode, LdbcNoResult.INSTANCE, operation);
				dbConnectionState.pushClient(client);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

    }

    public static class LdbcUpdate6AddPostHandler implements OperationHandler<LdbcUpdate6AddPost, TuGraphDbConnectionState> {

		@Override
		public void executeOperation(LdbcUpdate6AddPost operation, TuGraphDbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {
			CustomDataOutputStream cdos = new CustomDataOutputStream();
			cdos.writeInt64(operation.getPostId());
			cdos.writeString(operation.getImageFile());
			cdos.writeInt64(operation.getCreationDate().getTime());
			cdos.writeString(operation.getLocationIp());
			cdos.writeString(operation.getBrowserUsed());
			cdos.writeString(operation.getLanguage());
			cdos.writeString(operation.getContent());
			cdos.writeInt32(operation.getLength());
			cdos.writeInt64(operation.getAuthorPersonId());
			cdos.writeInt64(operation.getForumId());
			cdos.writeInt64(operation.getCountryId());
			cdos.writeInt16((short)operation.getTagIds().size());
			for (long tagId : operation.getTagIds()) {
				cdos.writeInt64(tagId);
			}
			try {
				TuGraphClient client = dbConnectionState.popClient();
				String op = "CALL_CPP_PLUGIN";
				String res = "interactive_update_6";
				String input = Base64.encodeBase64String(cdos.toByteArray());
				CustomDataInputStream cdis = client.call(op,  res, input);
				short resultCode = cdis.readInt16();
				resultReporter.report(resultCode, LdbcNoResult.INSTANCE, operation);
				dbConnectionState.pushClient(client);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

    }

    public static class LdbcUpdate7AddCommentHandler implements OperationHandler<LdbcUpdate7AddComment, TuGraphDbConnectionState> {

		@Override
		public void executeOperation(LdbcUpdate7AddComment operation, TuGraphDbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {
			CustomDataOutputStream cdos = new CustomDataOutputStream();
			cdos.writeInt64(operation.getCommentId());
			cdos.writeInt64(operation.getCreationDate().getTime());
			cdos.writeString(operation.getLocationIp());
			cdos.writeString(operation.getBrowserUsed());
			cdos.writeString(operation.getContent());
			cdos.writeInt32(operation.getLength());
			cdos.writeInt64(operation.getAuthorPersonId());
			cdos.writeInt64(operation.getCountryId());
			cdos.writeInt64(operation.getReplyToPostId());
			cdos.writeInt64(operation.getReplyToCommentId());
			cdos.writeInt16((short)operation.getTagIds().size());
			for (long tagId : operation.getTagIds()) {
				cdos.writeInt64(tagId);
			}
			try {
				TuGraphClient client = dbConnectionState.popClient();
				String op = "CALL_CPP_PLUGIN";
				String res = "interactive_update_7";
				String input = Base64.encodeBase64String(cdos.toByteArray());
				CustomDataInputStream cdis = client.call(op,  res, input);
				short resultCode = cdis.readInt16();
				resultReporter.report(resultCode, LdbcNoResult.INSTANCE, operation);
				dbConnectionState.pushClient(client);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

    }

    public static class LdbcUpdate8AddFriendshipHandler implements OperationHandler<LdbcUpdate8AddFriendship, TuGraphDbConnectionState> {

		@Override
		public void executeOperation(LdbcUpdate8AddFriendship operation, TuGraphDbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {
			CustomDataOutputStream cdos = new CustomDataOutputStream();
			cdos.writeInt64(operation.getPerson1Id());
			cdos.writeInt64(operation.getPerson2Id());
			cdos.writeInt64(operation.getCreationDate().getTime());
			try {
				TuGraphClient client = dbConnectionState.popClient();
				String op = "CALL_CPP_PLUGIN";
				String res = "interactive_update_8";
				String input = Base64.encodeBase64String(cdos.toByteArray());
				CustomDataInputStream cdis = client.call(op,  res, input);
				short resultCode = cdis.readInt16();
				resultReporter.report(resultCode, LdbcNoResult.INSTANCE, operation);
				dbConnectionState.pushClient(client);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

    }

}
