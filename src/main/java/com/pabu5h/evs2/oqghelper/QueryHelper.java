package com.pabu5h.evs2.oqghelper;

import com.xt.utils.DateTimeUtil;
import com.xt.utils.MathUtil;
import com.xt.utils.SqlUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

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

    public String getMerterSnFromMeterDisplayname(String meterDisplayname) {
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
}