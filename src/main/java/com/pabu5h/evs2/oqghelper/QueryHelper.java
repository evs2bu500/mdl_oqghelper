package com.pabu5h.evs2.oqghelper;

import com.pabu5h.evs2.dto.MeterInfoDto;
import com.xt.utils.DateTimeUtil;
import com.xt.utils.MathUtil;
import com.xt.utils.SqlUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Collections.singletonMap;

@Service
public class QueryHelper {

    @Autowired
    private OqgHelper oqgHelper;

    private final Logger logger;

    public QueryHelper(Logger logger) {
        this.logger = logger;
    }
    public List<String> getNewerMeterSnsFromTariffTable(){

        List<Map<String, Object>> meterSns = new ArrayList<>();

        // get all unique meterSns,
        // meterSn must be at least 10 characters long
        // and must start with 2018, 2019, 2020, 2021 and above
        String sql = "select distinct meter_sn from tariff " +
                "where meter_sn is not null and length(meter_sn) > 10 " +
                "and (meter_sn like '2018%' " +
                "  or meter_sn like '2019%' " +
                "  or meter_sn like '2020%' " +
                "  or meter_sn like '2021%'" +
                "  or meter_sn like '2022%'" +
                "  or meter_sn like '2023%'" +
                "  or meter_sn like '2024%'" +
                "  or meter_sn like '2025%'" +
                "  or meter_sn like '2026%'" +
                "  or meter_sn like '2027%'" +
                "  or meter_sn like '2028%'" +
                "  or meter_sn like '2029%'" +
                "  or meter_sn like '2030%')";

        try {
            meterSns = oqgHelper.OqgR(sql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return meterSns.stream().map(meterSn -> meterSn.get("meter_sn").toString()).toList();
    }

    public List<String> getActiveMeterSns(String tableName){
        String timekey = "kwh_timestamp";
        if(tableName ==null || tableName.isEmpty()){
            tableName = "meter_tariff";
            timekey = "tariff_timestamp";
        }

        if(tableName.equals("meter_tariff")){
            timekey = "tariff_timestamp";
        }

        List<Map<String, Object>> meterSns = new ArrayList<>();

        String sgNow = DateTimeUtil.getZonedDateTimeStr(LocalDateTime.now(), ZoneId.of("Asia/Singapore"));
        String sql = "select distinct meter_sn from " + tableName +
                " where " + timekey + " > timestamp '" + sgNow + "' - interval '24 hours' ";

        try {
            meterSns = oqgHelper.OqgR(sql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return meterSns.stream().map(meterSn -> meterSn.get("meter_sn").toString()).toList();
    }

    public Map<String, Object> getMeterInfo(String meterSnStr){
        String sql = "select * from meter where meter_sn = '" + meterSnStr + "'";
        List<Map<String, Object>> meterInfo = new ArrayList<>();
        try {
            meterInfo = oqgHelper.OqgR(sql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(meterInfo.size() == 0){
            return Map.of("info", "meter not found");
        }
        return meterInfo.get(0);
    }
    public List<Map<String, Object>> getAllMeterInfo(){
        String sql = "select meter_sn, meter_displayname, reading_interval, commission_timestamp" +
                " unit, street, block, building" +
                " from meter" +
                " inner join premise p on meter.premise_id = p.id";
        List<Map<String, Object>> meterInfo = new ArrayList<>();
        try {
            meterInfo = oqgHelper.OqgR(sql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return meterInfo;
    }

    public long getActiveMeterCount(String tableName){
        String timekey = "kwh_timestamp";
        if(tableName ==null || tableName.isEmpty()){
            tableName = "meter_tariff";
            timekey = "tariff_timestamp";
        }

        if(tableName.equals("meter_tariff")){
            timekey = "tariff_timestamp";
        }

        List<Map<String, Object>> count = new ArrayList<>();

        String sgNow = DateTimeUtil.getZonedDateTimeStr(LocalDateTime.now(), ZoneId.of("Asia/Singapore"));
        String sql = "select count(distinct meter_sn) from " + tableName +
                " where " + timekey + " > timestamp '" + sgNow + "' - interval '24 hours' ";

        try {
            count = oqgHelper.OqgR(sql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(count.size() == 0){
            return 0;
        }
        return Long.parseLong(count.get(0).get("count").toString());
    }

    public double getActiveKwhConsumption(String meterSnStr){
        List<Map<String, Object>> kwhConsumption = new ArrayList<>();

        String sgNow = DateTimeUtil.getZonedDateTimeStr(LocalDateTime.now(), ZoneId.of("Asia/Singapore"));
        String sql = "select sum(kwh_diff) as kwh_total from meter_tariff " +
                " where meter_sn = '" + meterSnStr + "' " +
                " and kwh_diff is not null " +
                " and tariff_timestamp > timestamp '" + sgNow + "' - interval '24 hours' ";

        try {
            kwhConsumption = oqgHelper.OqgR(sql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(kwhConsumption.size() == 0){
            return 0;
        }
        //sum() will always return a value, even if there is no data, the list will still have 1 element
        if(kwhConsumption.get(0).get("kwh_total") == null){
            return 0;
        }
        return Double.parseDouble(kwhConsumption.get(0).get("kwh_total").toString());
    }

    public double getAllActiveKwhConsumption(){
        List<Map<String, Object>> kwhConsumption = new ArrayList<>();

        String sgNow = DateTimeUtil.getZonedDateTimeStr(LocalDateTime.now(), ZoneId.of("Asia/Singapore"));
        String sql = "select sum(kwh_diff) as kwh_total from meter_tariff " +
                " where kwh_diff is not null " +
                " and tariff_timestamp > timestamp '" + sgNow + "' - interval '24 hours' ";

        try {
            kwhConsumption = oqgHelper.OqgR(sql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(kwhConsumption.size() == 0){
            return 0;
        }
        //sum() will always return a value, even if there is no data, the list will still have 1 element
        if(kwhConsumption.get(0).get("kwh_total") == null){
            return 0;
        }
        return Double.parseDouble(kwhConsumption.get(0).get("kwh_total").toString());
    }
    public double getAllTopupAmount(){
        List<Map<String, Object>>topupTotal = new ArrayList<>();

        String sgNow = DateTimeUtil.getZonedDateTimeStr(LocalDateTime.now(), ZoneId.of("Asia/Singapore"));
//        String sql = "select sum(credit_amt) as credit_total from meter_tariff " +
//                " where credit_amt is not null " +
//                " and tariff_timestamp > timestamp '" + sgNow + "' - interval '24 hours' ";
        String sql = "select sum(topup_amt) as topup_total from transaction_log " +
                " where topup_amt is not null " +
                " and transaction_status = 3 " +
                " and payment_mode != 4" +
                " and response_timestamp > timestamp '" + sgNow + "' - interval '24 hours' ";
        try {
            topupTotal = oqgHelper.OqgR(sql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(topupTotal.size() == 0){
            return 0;
        }
        //sum() will always return a value, even if there is no data, the list will still have 1 element
        if(topupTotal.get(0).get("topup_total") == null){
            return 0;
        }
        return Double.parseDouble(topupTotal.get(0).get("topup_total").toString());
    }


    public List<String> getAllMeterSns(String tableName){
        if(tableName ==null || tableName.isEmpty()){
            tableName = "meter_tariff";
        }

        List<Map<String, Object>> meterSns = new ArrayList<>();

        String sgNow = DateTimeUtil.getZonedDateTimeStr(LocalDateTime.now(), ZoneId.of("Asia/Singapore"));
        String sql = "select distinct meter_sn from " + tableName;

        try {
            meterSns = oqgHelper.OqgR(sql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return meterSns.stream().map(meterSn -> meterSn.get("meter_sn").toString()).toList();
    }

//    public List<String> getMeterSnsFromMeterTariffTable(){
//        List<Map<String, Object>> meterSns = new ArrayList<>();
//
//        String sql = "select distinct meter_sn from meter_tariff";
//
//        try {
//            meterSns = oqgHelper.OqgR(sql);
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//        return meterSns.stream().map(meterSn -> meterSn.get("meter_sn").toString()).toList();
//    }

    public String getMeterSnFromMeterDisplayname(String meterDisplayname) {
        String sqlMeterSn = "select meter_sn from meter where meter_displayname = '" + meterDisplayname + "'";
        List<Map<String, Object>> meterSn = new ArrayList<>();
        try {
            meterSn = oqgHelper.OqgR(sqlMeterSn);
        } catch (Exception e) {
            logger.error("Error getting meter_sn for meterDisplayname: " + meterDisplayname);
            throw new RuntimeException(e);
        }
        if (meterSn.isEmpty()) {
            logger.info("meter_sn is empty for meterDisplayname: " + meterDisplayname);
            return "";
        }
        return (meterSn.get(0).get("meter_sn") == null ? "" : meterSn.get(0).get("meter_sn").toString());
    }

    public ResponseEntity<Map<String, Object>> getMeterSnFromDisplayName(Map<String, String> reqMeterDisplayName){
        if(!reqMeterDisplayName.containsKey("meter_displayname")){
            return ResponseEntity.badRequest()
                    .body(Collections.singletonMap("error", "meter_displayname not found"));
        }
        String meterDisplayName = reqMeterDisplayName.get("meter_displayname");
        if(meterDisplayName.isBlank()){
            return ResponseEntity.badRequest()
                    .body(Collections.singletonMap("error", "meter_displayname is blank"));
        }

        String meterSnStr = "";
        try {
            meterSnStr = getMeterSnFromMeterDisplayname(meterDisplayName);
        }catch (Exception e){
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Collections.singletonMap("error", "Error getting meter_sn for meterDisplayname: " + meterDisplayName));
        }

        if(meterSnStr.isBlank()){
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Collections.singletonMap("info", "meter_sn is not found for meterDisplayname: " + meterDisplayName));
        }

        return ResponseEntity.ok(Collections.singletonMap("meter_sn", meterSnStr));
    }

    public Map<String, Object> getLatestMeterCredit(String meterSnStr, String tableName){
        if(tableName == null || tableName.isBlank()){
            tableName = "meter_tariff";
        }
        String sqlMeterCredit = "select ref_bal from " + tableName + " where meter_sn = '" + meterSnStr + "'" +
                " and ref_bal is not null order by id desc limit 1";
        List<Map<String, Object>> meterCredit = new ArrayList<>();
        try {
            meterCredit = oqgHelper.OqgR(sqlMeterCredit);
        } catch (Exception e) {
            logger.error("Error getting credit for meterSn: " + meterSnStr);
            throw new RuntimeException(e);
        }
        if(meterCredit.isEmpty()){
            logger.info("ref_bal is empty for meterSn: " + meterSnStr);
            return Collections.singletonMap("info", "ref_bal is empty for meterSn: " + meterSnStr);
        }
        return meterCredit.get(0);
    }

    public List<Long> getConcentratorIDs(){
        String sqlConcentrator = "select id from concentrator";
        List<Map<String, Object>> concentratorIds = new ArrayList<>();
        try {
            concentratorIds = oqgHelper.OqgR(sqlConcentrator);
        } catch (Exception e) {
            logger.error("Error getting concentrator IDs");
            throw new RuntimeException(e);
        }
        if(concentratorIds.isEmpty()){
            logger.info("no concentrator IDs found");
            return new ArrayList<>();
        }
        return concentratorIds.stream().map(concentratorId -> MathUtil.ObjToLong(concentratorId.get("id"))).toList();
    }

    public long getConcentratorIdFromMeterSn(String meterSnStr){
        String sqlConcentrator = "select concentrator_id from meter where meter_sn = '" + meterSnStr + "'";
        List<Map<String, Object>> concentrator = new ArrayList<>();
        try {
            concentrator = oqgHelper.OqgR(sqlConcentrator);
        } catch (Exception e) {
            logger.error("Error getting concentrator_id for meterSn: " + meterSnStr);
            throw new RuntimeException(e);
        }
        if(concentrator.isEmpty()){
            logger.info("concentrator is empty for meterSn: " + meterSnStr);
            return -1;
        }
        return MathUtil.ObjToLong(concentrator.get(0).get("concentrator_id")==null?-1:concentrator.get(0).get("concentrator_id"));
    }
    public Map<Long, Map<String, Object>> getConcentratorTariff(){
        //get a list of concentrator_id from concentrator table
        String sqlConcentrator = "select id from concentrator";
        List<Map<String, Object>> concentrators = new ArrayList<>();
        try {
            concentrators = oqgHelper.OqgR(sqlConcentrator);
        } catch (Exception e) {
            logger.error("Error getting concentrator list");
            throw new RuntimeException(e);
        }

        Map<Long, Map<String, Object>> concentratorTariff = new HashMap<>();

        for(Map<String, Object> concentrator : concentrators) {
            long concentratorId = MathUtil.ObjToLong(concentrator.get("id"));

            //get tariff for this concentrator_id from concentrator_tariff table
            String sqlTariff = "select offer_id, tariff_price from concentrator_tariff where concentrator_id = " + concentratorId;
            List<Map<String, Object>> tariff = new ArrayList<>();
            try {
                tariff = oqgHelper.OqgR(sqlTariff);
            } catch (Exception e) {
                logger.error("Error getting tariff for concentratorId: " + concentratorId);
                throw new RuntimeException(e);
            }
            if(tariff.isEmpty()){
                logger.info("tariff is empty for concentratorId: " + concentratorId);
                continue;
            }
            concentratorTariff.put(concentratorId,
                    Map.of("tariff_price", MathUtil.ObjToDouble(tariff.get(0).get("tariff_price")),
                            "offer_id", MathUtil.ObjToLong(tariff.get(0).get("offer_id"))));
        }
        return concentratorTariff;
    }
    public boolean meterSnExistsInMeterTable(String meterSnStr){
        String sql = "select id from meter where meter_sn = '" + meterSnStr + "'";
        List<Map<String, Object>> meter = new ArrayList<>();
        try {
            meter = oqgHelper.OqgR(sql);
        } catch (Exception e) {
            logger.error("Error getting meter for meterSn: " + meterSnStr);
            throw new RuntimeException(e);
        }
        return !meter.isEmpty();
    }

    public Map<String, Object> getEpochRefBalTimeStamp(String meterSnStr, String tableName){
        if(tableName == null || tableName.isBlank()){
            tableName = "meter_tariff";
        }
        String sqlMeterCredit = "select tariff_timestamp from " + tableName + " where meter_sn = '" + meterSnStr + "'" +
                " and ref_bal_tag = 'ref_bal_epoch' order by id desc limit 1";
        List<Map<String, Object>> meterCredit = new ArrayList<>();
        try {
            meterCredit = oqgHelper.OqgR(sqlMeterCredit);
        } catch (Exception e) {
            logger.error("Error getting credit for meterSn: " + meterSnStr);
            throw new RuntimeException(e);
        }
        if(meterCredit.isEmpty()){
            logger.info("epoch_ref_bal is empty for meterSn: " + meterSnStr);
            return Collections.singletonMap("info", "epoch_ref_bal is empty for meterSn: " + meterSnStr);
        }
        return meterCredit.get(0);
    }

    public void postMeterKiv(String meterSnStr,
                             String kivTag,
                             String postDateTimeStr,
                             long numOfEvents,
                             String postedBy,
                             String sessionId){
        String meterKivTable = "meter_kiv";

        if(postDateTimeStr == null || postDateTimeStr.isEmpty()) {
            postDateTimeStr = DateTimeUtil.getZonedDateTimeStr(LocalDateTime.now(), ZoneId.of("Asia/Singapore"));;
        }
        if(sessionId == null || sessionId.isEmpty()) {
            sessionId = UUID.randomUUID().toString();
        }

        if(!meterSnExistsInMeterTable(meterSnStr)){
            logger.info("meter_sn: " + meterSnStr + " does not exist in meter table");
            return;
        }

        //check if the record for that session_id exists
        String sqlCheck = "select id, number_of_events from " + meterKivTable +
                " where meter_sn = '" + meterSnStr +
                "' and session_id = '" + sessionId + "'";
        List<Map<String, Object>> meterKiv = new ArrayList<>();
        try {
            meterKiv = oqgHelper.OqgR(sqlCheck);
        } catch (Exception e) {
            logger.error("Error getting meterKiv for meterSn: " + meterSnStr);
            throw new RuntimeException(e);
        }
        if(meterKiv.isEmpty()) {
            Map<String, Object> kivSqlMap = Map.of("table", meterKivTable,
                    "content", Map.of(
                            "meter_sn", meterSnStr,
                            "kiv_tag", kivTag,
                            "kiv_start_timestamp", postDateTimeStr,
                            "number_of_events", numOfEvents,
                            "kiv_status","posted",
                            "posted_by", postedBy,
                            "session_id", sessionId
                    ));
            Map<String, String> sqlInsert = SqlUtil.makeInsertSql(kivSqlMap);
            if(sqlInsert.get("sql")==null){
                logger.error("Error getting insert sql for meterKiv for meterSn: " + meterSnStr);
                throw new RuntimeException("Error getting insert sql for meterKiv for meterSn: " + meterSnStr);
            }
            try {
                oqgHelper.OqgIU(sqlInsert.get("sql"));
            } catch (Exception e) {
                logger.error("Error inserting meterKiv for meterSn: " + meterSnStr);
                throw new RuntimeException(e);
            }
        } else {
            //update the record
            long id = MathUtil.ObjToLong(meterKiv.get(0).get("id"));
            long numOfEventsOld = MathUtil.ObjToLong(meterKiv.get(0).get("number_of_events")==null?0:meterKiv.get(0).get("number_of_events"));
            long numOfEventsNew = numOfEventsOld + numOfEvents;
            String sqlUpdate = "update " + meterKivTable + " set number_of_events = " + numOfEventsNew +
                    " where id = " + id;
            try {
                oqgHelper.OqgIU(sqlUpdate);
            } catch (Exception e) {
                logger.error("Error updating meterKiv for meterSn: " + meterSnStr);
                throw new RuntimeException(e);
            }
        }
    }
    public void postMeterKiv2(String meterSnStr,
                              String kivTag,
                              String postDateTimeStr,
                              long numOfEvents,
                              String kivRef,
                              double kivVal,
                              String postedBy,
                              String sessionId){
        String meterKivTable = "meter_kiv";

        if(postDateTimeStr == null || postDateTimeStr.isEmpty()) {
            postDateTimeStr = DateTimeUtil.getZonedDateTimeStr(LocalDateTime.now(), ZoneId.of("Asia/Singapore"));;
        }
        if(sessionId == null || sessionId.isEmpty()) {
            sessionId = UUID.randomUUID().toString();
        }

        if(!meterSnExistsInMeterTable(meterSnStr)){
            logger.info("meter_sn: " + meterSnStr + " does not exist in meter table");
            return;
        }

        //check if the record for that session_id exists
        String sqlCheck = "select id, number_of_events from " + meterKivTable +
                " where meter_sn = '" + meterSnStr +
                "' and session_id = '" + sessionId + "'";
        List<Map<String, Object>> meterKiv = new ArrayList<>();
        try {
            meterKiv = oqgHelper.OqgR(sqlCheck);
        } catch (Exception e) {
            logger.error("Error getting meterKiv for meterSn: " + meterSnStr);
            throw new RuntimeException(e);
        }
        if(meterKiv.isEmpty()) {
            Map<String, Object> kivSqlMap = Map.of("table", meterKivTable,
                    "content", Map.of(
                            "meter_sn", meterSnStr,
                            "kiv_tag", kivTag,
                            "kiv_start_timestamp", postDateTimeStr,
                            "number_of_events", numOfEvents,
                            "kiv_status","posted",
                            "kiv_ref", kivRef,
                            "kiv_val", kivVal,
                            "posted_by", postedBy,
                            "session_id", sessionId
                    ));
            Map<String, String> sqlInsert = SqlUtil.makeInsertSql(kivSqlMap);
            if(sqlInsert.get("sql")==null){
                logger.error("Error getting insert sql for meterKiv for meterSn: " + meterSnStr);
                throw new RuntimeException("Error getting insert sql for meterKiv for meterSn: " + meterSnStr);
            }
            try {
                oqgHelper.OqgIU(sqlInsert.get("sql"));
            } catch (Exception e) {
                logger.error("Error inserting meterKiv for meterSn: " + meterSnStr);
                throw new RuntimeException(e);
            }
        } else {
            //update the record
            long id = MathUtil.ObjToLong(meterKiv.get(0).get("id"));
            long numOfEventsOld = MathUtil.ObjToLong(meterKiv.get(0).get("number_of_events")==null?0:meterKiv.get(0).get("number_of_events"));
            long numOfEventsNew = numOfEventsOld + numOfEvents;
            String sqlUpdate = "update " + meterKivTable + " set number_of_events = " + numOfEventsNew +
                    " where id = " + id;
            try {
                oqgHelper.OqgIU(sqlUpdate);
            } catch (Exception e) {
                logger.error("Error updating meterKiv for meterSn: " + meterSnStr);
                throw new RuntimeException(e);
            }
        }
    }

    public long findMeterReadingInterval(String meterSnStr){
        String sql = "select reading_interval from meter_tariff where meter_sn = '" + meterSnStr + "'" +
                " and kwh_diff is not null " +
                " and reading_interval is not null " +
                " order by tariff_timestamp desc " +
                " limit 13";
        List<Map<String, Object>> intervals = new ArrayList<>();
        try {
            intervals = oqgHelper.OqgR(sql);
        } catch (Exception e) {
            logger.error("Error getting meter tariff for meterSn: " + meterSnStr);
            throw new RuntimeException(e);
        }
        if(intervals.isEmpty()){
            return 0;
        }
        List<Double> intervalList = new ArrayList<>();
        intervalList = intervals.stream().map(m -> MathUtil.ObjToDouble(m.get("reading_interval"))/60).collect(Collectors.toList());
        return MathUtil.findDominantLong(intervalList);
    }
    public void updateMeterReadingInterval(String meterSnStr, long interval){
        String sql = "update meter set reading_interval = " + interval +
                " where meter_sn = '" + meterSnStr + "'" ;
        try {
            oqgHelper.OqgIU(sql);
        } catch (Exception e) {
            logger.error("Error updating meter tariff for meterSn: " + meterSnStr);
            throw new RuntimeException(e);
        }
    }

    public Map<String, Object> getRecentMeterKiv(){
        List<Map<String, Object>>meterKiv = new ArrayList<>();

        String sgNow = DateTimeUtil.getZonedDateTimeStr(LocalDateTime.now(), ZoneId.of("Asia/Singapore"));
        String sql = "select * from meter_kiv " +
                " where kiv_tag != 'missing_ref_bal_epoch' " +
                " and kiv_start_timestamp > timestamp '" + sgNow + "' - interval '72 hours' "
                + " order by kiv_start_timestamp desc";
        try {
            meterKiv = oqgHelper.OqgR(sql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return Map.of("meter_kiv", meterKiv);
    }

    public void postOpLog(String postDateTimeStr,
                          String username,
                          String target,
                          String operation,
                          String targetSpec,
                          String opRef,
                          double opVal,
                          String remark,
                          String sessionId){
        String opsLogTable = "evs2_op_log";

        if(postDateTimeStr == null || postDateTimeStr.isEmpty()) {
            postDateTimeStr = DateTimeUtil.getZonedDateTimeStr(LocalDateTime.now(), ZoneId.of("Asia/Singapore"));;
        }
        if(sessionId == null || sessionId.isEmpty()) {
            sessionId = UUID.randomUUID().toString();
        }

        Map<String, Object> oplogSqlMap = Map.of("table", opsLogTable,
                "content", Map.of(
                        "op_timestamp", postDateTimeStr,
                        "username", username,
                        "evs2_acl_target", target,
                        "target_spec", targetSpec,
                        "evs2_acl_operation", operation,
                        "op_ref", opRef,
                        "op_val", opVal,
                        "remark", remark,
                        "session_id", sessionId
                ));
        Map<String, String> sqlInsert = SqlUtil.makeInsertSql(oplogSqlMap);
        if(sqlInsert.get("sql")==null){
            logger.error("Error getting insert sql for opsLogTable");
            throw new RuntimeException("Error getting insert sql for opsLogTable");
        }
        try {
            oqgHelper.OqgIU(sqlInsert.get("sql"));
        } catch (Exception e) {
            logger.error("Error inserting opsLogTable");
            throw new RuntimeException(e);
        }
    }

}