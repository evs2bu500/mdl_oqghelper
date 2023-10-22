package com.pabu5h.evs2.oqghelper;

import com.xt.utils.DateTimeUtil;
import com.xt.utils.MathUtil;
import com.xt.utils.SqlUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.time.ZoneId;
import java.util.*;
import java.util.logging.Logger;

import static java.time.LocalDateTime.now;

@Service
public class QueryHelperDep {
    private final Logger logger = Logger.getLogger(QueryHelper.class.getName());
    @Autowired
    private OqgHelper oqgHelper;
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

    public void postMeterKiv(String meterSnStr,
                             String kivTag,
                             String postDateTimeStr,
                             long numOfEvents,
                             String postedBy,
                             String sessionId){
        String meterKivTable = "meter_kiv";

        if(postDateTimeStr == null || postDateTimeStr.isEmpty()) {
            postDateTimeStr = DateTimeUtil.getZonedDateTimeStr(now(), ZoneId.of("Asia/Singapore"));;
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
            logger.info("Error getting meterKiv for meterSn: " + meterSnStr);
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
                logger.info("Error getting insert sql for meterKiv for meterSn: " + meterSnStr);
                throw new RuntimeException("Error getting insert sql for meterKiv for meterSn: " + meterSnStr);
            }
            try {
                oqgHelper.OqgIU(sqlInsert.get("sql"));
            } catch (Exception e) {
                logger.info("Error inserting meterKiv for meterSn: " + meterSnStr);
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
                logger.info("Error updating meterKiv for meterSn: " + meterSnStr);
                throw new RuntimeException(e);
            }
        }
    }
    public boolean meterSnExistsInMeterTable(String meterSnStr){
        String sql = "select id from meter where meter_sn = '" + meterSnStr + "'";
        List<Map<String, Object>> meter = new ArrayList<>();
        try {
            meter = oqgHelper.OqgR(sql);
        } catch (Exception e) {
            logger.info("Error getting meter for meterSn: " + meterSnStr);
            throw new RuntimeException(e);
        }
        return !meter.isEmpty();
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
            postDateTimeStr = DateTimeUtil.getZonedDateTimeStr(now(), ZoneId.of("Asia/Singapore"));;
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
            logger.info("Error getting insert sql for opsLogTable");
            throw new RuntimeException("Error getting insert sql for opsLogTable");
        }
        try {
            oqgHelper.OqgIU(sqlInsert.get("sql"));
        } catch (Exception e) {
            logger.info("Error inserting opsLogTable");
            throw new RuntimeException(e);
        }
    }

//    public ResponseEntity<Map<String, Object>> getMeterSnFromDisplayName(Map<String, String> reqMeterDisplayName){
//        if(!reqMeterDisplayName.containsKey("meter_displayname")){
//            return ResponseEntity.badRequest()
//                    .body(Collections.singletonMap("error", "meter_displayname not found"));
//        }
//        String meterDisplayName = reqMeterDisplayName.get("meter_displayname");
//        if(meterDisplayName.isBlank()){
//            return ResponseEntity.badRequest()
//                    .body(Collections.singletonMap("error", "meter_displayname is blank"));
//        }
//
//        String meterSnStr = "";
//        try {
//            meterSnStr = getMeterSnFromMeterDisplayname(meterDisplayName);
//        }catch (Exception e){
//            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
//                    .body(Collections.singletonMap("error", "Error getting meter_sn for meterDisplayname: " + meterDisplayName));
//        }
//
//        if(meterSnStr.isBlank()){
//            return ResponseEntity.status(HttpStatus.NOT_FOUND)
//                    .body(Collections.singletonMap("info", "meter_sn is not found for meterDisplayname: " + meterDisplayName));
//        }
//
//        return ResponseEntity.ok(Collections.singletonMap("meter_sn", meterSnStr));
//    }

}
