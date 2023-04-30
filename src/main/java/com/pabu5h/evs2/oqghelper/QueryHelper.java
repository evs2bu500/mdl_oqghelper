package com.pabu5h.evs2.oqghelper;

import com.xt.utils.MathUtil;
import com.xt.utils.SqlUtil;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class QueryHelper {

    @Autowired
    private OqgHelper oqgHelper;

    public final Logger logger;

    QueryHelper(Logger logger) {
        this.logger = logger;
    }

    public String getMerterSnFromMeterDisplayname(String meterDisplayname) {
        String sqlMeterSn = "select meter_sn from meter where display_name = '" + meterDisplayname + "'";
        List<Map<String, Object>> meterSn = new ArrayList<>();
        try {
            meterSn = oqgHelper.OqgR(sqlMeterSn);
        } catch (Exception e) {
//            logger.error("Error getting meter_sn for meterDisplayname: " + meterDisplayname);
            throw new RuntimeException(e);
        }
        if (meterSn.isEmpty()) {
//            logger.info("meter_sn is empty for meterDisplayname: " + meterDisplayname);
            return "";
        }
        return (meterSn.get(0).get("meter_sn") == null ? "" : meterSn.get(0).get("meter_sn").toString());
    }

    public void postMeterKiv(String meterSnStr,
                             String kivTag,
                             String postDateTimeStr,
                             long numOfEvents,
                             String postedBy,
                             String sessionId){
        String meterKivTable = "meter_kiv";

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
}