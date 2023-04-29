package com.pabu5h.evs2.oqghelper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class QueryHelper {

    @Autowired
    private OqgHelper oqgHelper;

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
}