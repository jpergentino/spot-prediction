package cloud.aws.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jfree.util.Log;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.AvailabilityZone;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.SpotPrice;

import cloud.aws.bean.MySpotPrice;
import cloud.aws.util.AWSUtil;
import cloud.aws.util.TimerUtil;
import core.db.PostgresDBConnection;
import core.exceptions.TooManyConnectionsException;
import core.executor.cbr.Case;

public class SpotDAO {
	
	private static final Logger log = LogManager.getLogger(SpotDAO.class);
	
	public static final String TB_SPOTPRICE = "spotprice_all";
	public static final String TB_CASES = "cases_all";

	private Connection postgresConnection;
	
	
	public SpotDAO() {
	}
	
	public void closeConnection() {
		try {
			PostgresDBConnection.closeConnection(postgresConnection);
		} catch (SQLException e) {
			log.error("Error closing db connection: "+ e.getMessage());
		}
	}
	
	public List<Case> findCases(String region, String zone, String instance, int dayOfWeek, int hourOfDay) throws SQLException {
		
//		log.debug("Searching "+ region +"/"+ zone +"/"+ instance +" in DOW: "+ dayOfWeek +" and HOD: "+ hourOfDay);
		
		if (StringUtils.isEmpty(region) && StringUtils.isEmpty(zone) && StringUtils.isEmpty(instance)) {
			throw new IllegalArgumentException("Region/Zone/Instance are required.");
		}
		
		String sql = new String("SELECT timeToRevocation, censored FROM "+ TB_CASES +" c "
				+ " WHERE c.region = ? AND c.zone = ? AND c.instance = ? AND c.dayOfWeek = ? AND hourOfDay = ? "
				+ " ORDER BY timeToRevocation");
		
		PreparedStatement ps = getMyConnection().prepareStatement(sql.toString());
		
		ps.setString(1, region);
		ps.setString(2, zone);
		ps.setString(3, instance);
		ps.setInt(4, dayOfWeek);
		ps.setInt(5, hourOfDay);
		
		ResultSet rs = ps.executeQuery();
		
		List<Case> listToReturn = new ArrayList<>();
		while (rs.next()) {
			listToReturn.add(new Case(rs.getInt(1), rs.getBoolean(2)));
		}
		rs.close();
		ps.close();
		getMyConnection().commit();
		
		return listToReturn;
	}
	
	public List<Case> findCases(String instance, int dayOfWeek, int hourOfDay) throws SQLException {
		
		if (StringUtils.isEmpty(instance)) {
			throw new IllegalArgumentException("Instance is required.");
		}
		
		String sql = new String("SELECT timeToRevocation, censored FROM "+ TB_CASES +" c "
				+ " WHERE c.instance = ? AND c.dayOfWeek = ? AND hourOfDay = ? "
				+ " ORDER BY timeToRevocation");
		
		PreparedStatement ps = getMyConnection().prepareStatement(sql.toString());
		
		ps.setString(1, instance);
		ps.setInt(2, dayOfWeek);
		ps.setInt(3, hourOfDay);
		
		ResultSet rs = ps.executeQuery();
		
		List<Case> listToReturn = new ArrayList<>();
		while (rs.next()) {
			listToReturn.add(new Case(rs.getInt(1), rs.getBoolean(2)));
		}
		rs.close();
		ps.close();
		
		return listToReturn;
	}
	
	private Connection getMyConnection() {
		boolean connected = false;
		while (!connected) {
			try {
				postgresConnection = PostgresDBConnection.getInstance().getConnectionFromPool();
				connected = true;
			} catch (TooManyConnectionsException e) {
				log.debug(e.getMessage() +". Retrying...");
				TimerUtil.sleep(1);
			}
		}

		return postgresConnection;
	}

	private void closeConnectionFromPool(Connection con) throws SQLException {
		PostgresDBConnection.closeConnection(con);
	}
	
	public List<MySpotPrice> findAll(String region, String zone, String instance) throws SQLException {
		return findAll(region, zone, instance, null, null);
	}
	
	public List<MySpotPrice> findAll(String region, String zone, String instance, Date initDate, Date limitDate) throws SQLException {
		
//		log.debug("Searching "+ region +"/"+ zone +"/"+ instance +" BETWEEN: "+ DateUtil.getFormattedDateTime(initDate) +" and "+ DateUtil.getFormattedDateTime(limitDate));

		if (region == null || zone == null || instance == null) {
			throw new IllegalArgumentException("Region, zone and instance are required.");
		}
		
		if ((initDate != null && limitDate == null) || (initDate == null && limitDate != null)) {
			throw new IllegalArgumentException("Both initDate and limitDate are required when one of them was passed as parameter.");
		}
		
		StringBuilder sql = new StringBuilder("SELECT zone, instance, price, timestamp FROM "+ TB_SPOTPRICE +" s ");
		sql.append(" WHERE s.region = ? AND s.zone = ? AND instance = ? ");
		
		if (initDate != null && limitDate != null) {
			sql.append(" AND s.timestamp BETWEEN ? AND ?");
		}

		sql.append(" ORDER BY timestamp");
		
		PreparedStatement ps = getMyConnection().prepareStatement(sql.toString());
		
		ps.setString(1, region);
		ps.setString(2, zone);
		ps.setString(3, instance);
		
		if (initDate != null || limitDate != null) {
			ps.setTimestamp(4, new Timestamp(initDate.getTime()));
			ps.setTimestamp(5, new Timestamp(limitDate.getTime()));
		}
		
		ResultSet rs = ps.executeQuery();
		
		List<MySpotPrice> listToReturn = new ArrayList<MySpotPrice>(100);
		while (rs.next()) {
			MySpotPrice s = new MySpotPrice();
			s.setAvailabilityZone(rs.getString("zone"));
			s.setInstanceType(InstanceType.fromValue(rs.getString("instance")));
			s.setSpotPrice(String.valueOf(rs.getDouble("price")));
			s.setTimestamp(rs.getTimestamp("timestamp"));
			listToReturn.add(s);
		}
		rs.close();
		ps.close();
		
		return listToReturn;
	}
	
	public Long getLastRecord(Regions region, AvailabilityZone zone, InstanceType instance) throws SQLException {
		Long returnValue = null;
		PreparedStatement ps = getMyConnection().prepareStatement("SELECT MAX(s.time) FROM "+ TB_SPOTPRICE +" s "+ 
				" WHERE s.region = ? AND s.zone = ? AND s.instance = ?");
		ps.setString(1, region.getName());
		ps.setString(2, zone.getZoneName());
		ps.setString(3, instance.toString());
		
		ResultSet rs = ps.executeQuery();
		if (rs.next()) {
			returnValue = rs.getLong(1) != 0 ? rs.getLong(1) : null;
		}
		rs.close();
		ps.close();
		
		return returnValue;
		
	}
	
	public void saveSpotCasesBatch(List<Case> caseList) throws SQLException {
		String sql = "INSERT INTO "+ TB_CASES +" "
				+ " (region, zone, instance, dayOfWeek, hourOfDay, censored, initTime, endTime, timeToRevocation, skipRecords, initValue, endValue, additionAllowed) "
				+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		
		Connection con = getMyConnection();
		PreparedStatement ps = con.prepareStatement(sql);
		
		for (Case c : caseList) {
			ps.setString(1, c.getRegion());
			ps.setString(2, c.getZone());
			ps.setString(3, c.getInstance());
			ps.setInt(4, c.getDayOfWeek());
			ps.setInt(5, c.getHourOfDay());
			ps.setBoolean(6, c.isCensored());
			ps.setLong(7, c.getInitTime());
			ps.setLong(8, c.getEndTime());
			ps.setInt(9, c.getTimeToRevocation());
			ps.setInt(10, c.getSkipRecords());
			ps.setDouble(11, c.getInitValue());
			ps.setDouble(12, c.getEndValue());
			ps.setDouble(13, c.getAdditionAllowed());
			
			ps.addBatch();
		}
		
		log.debug("Saving "+ caseList.size() +" Cases of "+ AWSUtil.fullInstanceName(caseList.get(0).getRegion(), caseList.get(0).getZone(), caseList.get(0).getInstance()));
		
		long l1 = System.currentTimeMillis();
		
		ps.executeBatch();
		con.commit();
		ps.close();
		closeConnectionFromPool(con);
		
		long l2 = System.currentTimeMillis();
		log.debug("Saved "+ caseList.size() +" Cases of "+ AWSUtil.fullInstanceName(caseList.get(0).getRegion(), caseList.get(0).getZone(), caseList.get(0).getInstance()) +" in "+ ((l2 - l1)/1000) +" seconds.");
	}
	
	public void saveSpotPriceBatch(List<SpotPrice> sps, Regions reg) throws SQLException {
		
		PreparedStatement ps = getMyConnection().prepareStatement("INSERT INTO "+ TB_SPOTPRICE +" "
				+ " (region, zone, instance, timestamp, price, time, execution_time) "
				+ " values (?, ?, ?, ?, ?, ?, ?) ");
		
		for (SpotPrice sp : sps) {
			ps.setString(1, reg.getName());
			ps.setString(2, sp.getAvailabilityZone());
			ps.setString(3, sp.getInstanceType());
			ps.setTimestamp(4, new Timestamp(sp.getTimestamp().getTime()));
			ps.setDouble(5, Double.valueOf(sp.getSpotPrice()));
			ps.setLong(6, Long.valueOf(sp.getTimestamp().getTime()));
			ps.setTimestamp(7, new Timestamp(AWSUtil.startedTime.getTimeInMillis()));
			
			ps.addBatch();
			
		}
		
		ps.executeBatch();
		getMyConnection().commit();
		ps.close();
		log.debug("Saved "+ sps.size() +" SpotPriceHistory of "+ AWSUtil.fullInstanceName(reg, sps.get(0).getAvailabilityZone(), sps.get(0).getInstanceType()));
		closeConnection();
	}
	
	public void saveSpotPrice(List<SpotPrice> spotPriceList, Regions reg) throws SQLException, TooManyConnectionsException {
		
		if (spotPriceList != null && !spotPriceList.isEmpty()) {
			log.debug("Saving "+ spotPriceList.size() +" SpotPriceHistory of "+ AWSUtil.fullInstanceName(reg, spotPriceList.get(0).getAvailabilityZone(), spotPriceList.get(0).getInstanceType()));
		}
		
		Connection con = getMyConnection();
		
		con.setAutoCommit(false);
		
		// Need to reverse because it comes from AWS from last to begin.
		List<SpotPrice> reverseList = spotPriceList.subList(0, spotPriceList.size());
		Collections.reverse(reverseList);
		
		int failureCount = 0;
		
		for (SpotPrice spotPrice : reverseList) {
			
			PreparedStatement ps = con.prepareStatement("INSERT INTO "+ TB_SPOTPRICE +" "
					+ " (region, zone, instance, timestamp, price, time, execution_time) "
					+ " values (?, ?, ?, ?, ?, ?, ?) ");
			
			ps.setString(1, reg.getName());
			ps.setString(2, spotPrice.getAvailabilityZone());
			ps.setString(3, spotPrice.getInstanceType());
			ps.setTimestamp(4, new Timestamp(spotPrice.getTimestamp().getTime()));
			ps.setDouble(5, Double.valueOf(spotPrice.getSpotPrice()));
			ps.setLong(6, Long.valueOf(spotPrice.getTimestamp().getTime()));
			ps.setTimestamp(7, new Timestamp(AWSUtil.startedTime.getTimeInMillis()));
			
			try {
				ps.executeUpdate();
			} catch (SQLIntegrityConstraintViolationException e) {
				failureCount++;
				log.debug("UNIQUE constraint violation: of "+ AWSUtil.fullInstanceName(reg, reverseList.get(0).getAvailabilityZone(), reverseList.get(0).getInstanceType()) +": "+ e.getMessage());
			} finally {
				ps.close();
			}
			
		}
		
		con.commit();
		closeConnectionFromPool(con);
		
		log.debug("Saved "+ (reverseList.size() - failureCount) + " in "+ AWSUtil.fullInstanceName(reg, reverseList.get(0).getAvailabilityZone(), reverseList.get(0).getInstanceType()) +". Duplicates: "+ failureCount);
		
	}
	
	public Map<Long, Double> findPriceChangeHistory(String region, String zone, String instance, Date dateInit, Date dateEnd ) {
		
		Map<Long, Double> mapPriceChanges = new TreeMap<>();
		
		int paramValidateCount = 0;
		paramValidateCount += dateInit != null ? 1 : 0;
		paramValidateCount += dateEnd != null ? 1 : 0;
		
		if (paramValidateCount != 2 && paramValidateCount != 0) {
			throw new IllegalArgumentException("Provide both dates if some of them was provided. "+ paramValidateCount);
		}
		
		StringBuilder sql = new StringBuilder("SELECT ");
		sql.append(" timestamp, price ");
		sql.append(" FROM "+ TB_SPOTPRICE +" ");
		sql.append(" WHERE true ");
		
		if (region != null && !region.isEmpty()) {
			sql.append(" AND region = ? ");
		}
		
		if (zone != null && !zone.isEmpty()) {
			sql.append(" AND zone = ? ");
		}
		
		if (instance != null && !instance.isEmpty()) {
			sql.append(" AND instance = ? ");
		}
		
		if (dateInit != null || dateEnd != null) {
			sql.append(" AND DATE(timestamp) BETWEEN ? AND ? ");
		}
		
		sql.append(" ORDER BY timestamp ");
		
		try (PreparedStatement ps = getMyConnection().prepareStatement(sql.toString())) {
			
			int paramCount = 0;
			
			if (region != null && !region.isEmpty()) {
				ps.setString(++paramCount, region);
			}
			
			if (zone != null && !zone.isEmpty()) {
				ps.setString(++paramCount, zone);
			}
			
			if (instance != null && !instance.isEmpty()) {
				ps.setString(++paramCount, instance);
			}
			
			if (dateInit != null || dateEnd != null) {
				Calendar cInit = Calendar.getInstance();
				cInit.setTime(dateInit);
				cInit.set(Calendar.HOUR_OF_DAY, 0);
				
				Calendar cEnd = Calendar.getInstance();
				cEnd.setTime(dateEnd);
				cEnd.set(Calendar.HOUR_OF_DAY, 0);
				
				ps.setDate(++paramCount, new java.sql.Date(cInit.getTimeInMillis()));
				ps.setDate(++paramCount, new java.sql.Date(cEnd.getTimeInMillis()));
			}
			
			try ( ResultSet rs = ps.executeQuery() ) {
				
				while(rs.next()) {
					mapPriceChanges.put(rs.getTimestamp(1).getTime(), rs.getDouble(2));
				}
				
			}
			
			
		} catch (SQLException e) {
			Log.error("Error on query: "+ e.getMessage() );
		}
		
		return mapPriceChanges;
		
	}	

	
}
