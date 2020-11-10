package com.ksr.avrodata;

import com.ksr.avrodata.ptr.fields100.*;
import com.sun.xml.internal.ws.encoding.soap.SerializationException;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.AvroSchema;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.UUID;

public class DataGen {
    public static void main(String args[]) throws IOException {

    }

    protected byte[] serializeImpl(String subject, Object object, AvroSchema schema) throws SerializationException, IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        DatumWriter<OMSmsgFields> writer = new SpecificDatumWriter<OMSmsgFields>(OMSmsgFields.class);
        OMSmsgFields omSmsgFields = getOmSMSgObject();
        writer.write(omSmsgFields, encoder);
        encoder.flush();
        return outputStream.toByteArray();
    }

    protected OMSmsgFields getOmSMSgObject() {
        String uniqueID = UUID.randomUUID().toString();
        Random ran = new Random();
        ByteBuffer price = ByteBuffer.allocate(4).put((byte) (ran.nextInt(10000) + 1));
        ByteBuffer quantity = ByteBuffer.allocate(4).put((byte) (ran.nextInt(1000) + 1));

        OMSmsgFields omSmsgFields = new OMSmsgFields();

        omSmsgFields.setSchemaName("lz_ork.avsc");
        omSmsgFields.setSchemaVersion(1);
        omSmsgFields.setTime(Instant.now());
        omSmsgFields.setMessageUid(uniqueID);
        omSmsgFields.setSource("TestDataGeneratorEBond");
        omSmsgFields.setPENDINGALLOCIND(true);
        omSmsgFields.setCLNTINTID(uniqueID.substring(0, 6));
        omSmsgFields.setCLNTINTID(CLNT_INT_ID_TYPES.ISIS.name());
        omSmsgFields.setBUYSELLOMS(BUY_SELL_OMS.BUYI);
        omSmsgFields.setFININSTRIDINT(uniqueID.substring(0, 6));
        omSmsgFields.setFININSTRIDINTTYPE(FIN_INSTR_ID_INT_TYPES.BBG);
        omSmsgFields.setPRICENOTATION(PRICE_NOTATION_TYPES.BAPO);
        omSmsgFields.setPRICECURRENCY("EUR");
        omSmsgFields.setCURRCODELEG2("EURR");
        omSmsgFields.setOPTIONTYPEOMS(OPTION_TYPES_OMS.CALL);
        omSmsgFields.setPRICE(price);
        omSmsgFields.setSTRIKEPRICECURRENCY(new ArrayList('E'));
        omSmsgFields.setUPFRONTPAYMENT(price);
        omSmsgFields.setSTTLMDELIVERYTYPE(STTLM_DELIVERY_TYPES.CASH);
        omSmsgFields.setOPTIONEXERCISESTYLE(OPTION_EXERCISE_STYLES.AMER);
        omSmsgFields.setMATURITYDATE(Instant.now());
        omSmsgFields.setEXPIRYDATE(Instant.now());
        omSmsgFields.setQTYCURRENCY("EUR");
        omSmsgFields.setQTYNOTATION(QTY_NOTATION_TYPES.MONE);
        omSmsgFields.setINITIALQUANTITY(price);
        omSmsgFields.setDIRECTEDORDERIND(false);
        omSmsgFields.setTRADINGCAPACITYIND(TRADING_CAPACITY_INDS.AOTC);
        omSmsgFields.setLIQPROVACTIVITY(true);
        omSmsgFields.setORDIDEXT("ORK-36");
        omSmsgFields.setORDIDEXTTYPE(ORD_ID_EXT_TYPES.BROK);
        omSmsgFields.setORDERIDINT("100001");
        omSmsgFields.setVENUEMIC("VENUEMIC");
        omSmsgFields.setORDERTYPE("ORDERTYPE");
        omSmsgFields.setLIMITPRICE(price);
        omSmsgFields.setADDTLLIMITPRICE(price);
        omSmsgFields.setSTOPPRICE(price);
        omSmsgFields.setPEGGEDLIMITPRICE(price);
        omSmsgFields.setREMAININGQUANTITY(quantity);
        omSmsgFields.setDISPLQUANTITY(quantity);
        omSmsgFields.setTRADEDQUANTITY(quantity);
        omSmsgFields.setMAQ(price);
        omSmsgFields.setMES(price);
        omSmsgFields.setMESFIRSTEXECONLYIND(false);
        omSmsgFields.setSELFEXECPREVENTIND(true);
        omSmsgFields.setSELFEXECPREVENTIND(true);
        omSmsgFields.setORDERSUBMTS(Instant.now());
        omSmsgFields.setORDERENTRYTS(Instant.now());
        omSmsgFields.setEXECUTIONTS(Instant.now());
        omSmsgFields.setORDEREVNTCODE(ORDER_EVNT_CODE_TYPES.EXCOR);
        omSmsgFields.setORDERACTN(ORDER_ACTN_TYPES.CACL);
        omSmsgFields.setSHORTSELLINGIND(SHORT_SELLING_INDS.SELL);
        omSmsgFields.setWAIVERIND(new ArrayList(Collections.singleton(WAIVER_IND_TYPES.valueOf("ESCB"))));
        omSmsgFields.setTRADINGVENUETRXID("TRX");
        omSmsgFields.setVALIDITYPERIODIND(VALIDITY_PERIOD_INDS.FORK);
        omSmsgFields.setORDERRESTRICTION(ORDER_RESTRICTION_TYPES.OTHR);
        omSmsgFields.setVALIDITYPERIODENDDATE(Instant.now());
        omSmsgFields.setAGGORDERIND(false);
        omSmsgFields.setSENDENTITYID("sendentity");
        omSmsgFields.setSENDENTITYIDTYPE(SEND_ENTITY_ID_TYPES.BPKN);
        omSmsgFields.setUNDLINSTRID(new ArrayList(Collections.singleton("UNDLINSTRID")));
        omSmsgFields.setUNDLINSTRIDTYPE(new ArrayList<>(Collections.singleton(UNDL_INSTR_ID_TYPES.BBG)));
        omSmsgFields.setEXECWITHINFIRM("EXECWITHINFIRM");
        omSmsgFields.setPRODUCTDESC("PRODUCT_DESC");
        omSmsgFields.setCONTRACTID("CONTRACT_ID");
        omSmsgFields.setCURRENCYPAIR("EUR");
        omSmsgFields.setINVESTDECISIONWITHINFIRM("INVEST_DECISION_WITHIN_FIRM");
        omSmsgFields.setCOUNTRYWHEREORDRECEIVED("COUNTRY_WHERE_ORD_RECEIVED");
        omSmsgFields.setACTONBEHALFCLIENTID("ACTONBEHALFCLIENTID");
        omSmsgFields.setACTONBEHALFCLIENTIDTYPE(ACT_ON_BEHALF_CLIENT_ID_TYPES.BPKN);
        omSmsgFields.setQUOTEVALIDUNTIL(Instant.now());
        omSmsgFields.setQUOTECXLREASON(QUOTE_CXL_REASONS.OTHR);
        omSmsgFields.setPTTEXMPTREASON(PTT_EXMPT_REASONS.ESCB);
        omSmsgFields.setSPREAD(quantity);
        omSmsgFields.setXCHANGERATE(quantity);
        omSmsgFields.setREFPRICE(price);
        omSmsgFields.setPAYNOTIONAL(price);
        omSmsgFields.setRCVNOTIONAL(price);
        omSmsgFields.setPAYNOTIONALCURRENCY("PAY_NOTIONAL_CURRENCY");
        omSmsgFields.setRCVNOTIONALCURRENCY("RCV_NOTIONAL_CURRENCY");
        omSmsgFields.setPAYNOTIONALCURRENCY("PAY_NOTIONAL_CURRENCY");
        omSmsgFields.setREPORATE(price);
        omSmsgFields.setSTRIKEPRICEPEND(false);
        omSmsgFields.setSTRIKEPRICENOTATION(new ArrayList<>(Collections.singleton(STRIKE_PRICE_NOTATION_TYPES.MONE)));
        omSmsgFields.setEXCHSEQNR(1000);
        omSmsgFields.setQUOTEACCEPTTIME(Instant.now());
        omSmsgFields.setCOUNTERPARTYID("COUNTERPARTY_ID");
        omSmsgFields.setCOUNTERPARTYIDTYPE(COUNTERPARTY_ID_TYPES.BPKN);
        omSmsgFields.setEXECIDINT("EXEC_ID_INT");
        omSmsgFields.setSHORTCODECLIENT("SHORTCODE_CLIENT");
        omSmsgFields.setSHORTCODEIDM("SHORTCODE_IDM");
        omSmsgFields.setSHORTCODEEDM("SHORTCODE_EDM");
        return omSmsgFields;
    }

}