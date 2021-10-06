package com.ldbc.impls.workloads.ldbc.snb.graphdb;

import com.ldbc.driver.DbException;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate2AddPostLike;
import com.ldbc.impls.workloads.ldbc.snb.db.BaseDb;
import com.ldbc.impls.workloads.ldbc.snb.graphdb.converter.GraphDBConverter;
import com.ldbc.impls.workloads.ldbc.snb.graphdb.operationhandlers.GraphDBListOperationHandler;
import com.ldbc.impls.workloads.ldbc.snb.graphdb.operationhandlers.GraphDBUpdateOperationHandler;

import org.eclipse.rdf4j.query.BindingSet;

import java.util.Map;

public class GraphDB extends BaseDb<GraphDBQueryStore> {
	@Override
	protected void onInit(Map<String, String> properties, LoggingService loggingService) throws DbException {

	}

	// Interactive complex reads

	public static class InteractiveQuery1 extends GraphDBListOperationHandler<LdbcQuery1, LdbcQuery1Result> {

		@Override
		public String getQueryString(GraphDBConnectionState state, LdbcQuery1 operation) {
			return state.getQueryStore().getQuery1(operation);
		}

		@Override
		public LdbcQuery1Result convertSingleResult(BindingSet bindingSet) {
			return null;
		}
	}


	public static class InteractiveQuery8 extends GraphDBListOperationHandler<LdbcQuery8, LdbcQuery8Result> {

		@Override
		public String getQueryString(GraphDBConnectionState state, LdbcQuery8 operation) {
			return state.getQueryStore().getQuery8(operation);
		}

		@Override
		public LdbcQuery8Result convertSingleResult(BindingSet bindingSet) {
			return new LdbcQuery8Result(
					Long.parseLong(bindingSet.getBinding("from").getValue().stringValue()),
					bindingSet.getBinding("first").getValue().stringValue(),
					bindingSet.getBinding("last").getValue().stringValue(),
					GraphDBConverter.stringTimestampToEpoch(bindingSet.getBinding("dt").getValue().stringValue()),
					Long.parseLong(bindingSet.getBinding("rep").getValue().stringValue()),
					bindingSet.getBinding("content").getValue().stringValue());
		}

	}

	// Interactive writes

	public static class Update2AddPostLike extends GraphDBUpdateOperationHandler<LdbcUpdate2AddPostLike> {

		@Override
		public String getQueryString(GraphDBConnectionState state, LdbcUpdate2AddPostLike operation) {
			return state.getQueryStore().getUpdate2(operation);
		}
	}

}
