package org.apache.flink.bili.external.keeper;

import lombok.extern.slf4j.Slf4j;

import java.util.EnumSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** keeper base response message. */
@Slf4j
public class KeeperMessage<T> {

    private Integer code;

    private String message;

    private String traceId;

    private T data;

    private static final String KEEPER_ERROR_CODE_IDENTIFY_REGEX = "-?[1-9]+[0-9]*";

    private static final EnumSet<KeeperCode> SUCCESS_CODES =
            EnumSet.of(KeeperCode.SUCCESS, KeeperCode.DEFAULT_SUCCESS);

    public static <T> KeeperMessage<T> requestFailedResponse() {
        return KeeperMessageBuilder.<T>anKeeperMessage()
                .withCode(KeeperCode.REQUEST_FAILED.getCode())
                .build();
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public static <T> boolean isSuccessful(KeeperMessage<T> keeperMessage) {
        if (keeperMessage == null) {
            return false;
        }

        return SUCCESS_CODES.stream()
                .map(KeeperCode::getCode)
                .collect(Collectors.toList())
                .contains(keeperMessage.getCode());
    }

    public static <T> String errorMsg(KeeperMessage<T> keeperMessage) {
        log.error(
                "call keeper api use open api encounter exception,VoyagerRuntimeException,code:{},message:{}",
                keeperMessage.code,
                keeperMessage.message);
        String message = keeperMessage.message;
        Pattern p = Pattern.compile(KEEPER_ERROR_CODE_IDENTIFY_REGEX);
        Matcher matcher = p.matcher(message);
        if (matcher.find()) {
            KeeperCode keeperCode = KeeperCode.fromCode(Integer.parseInt(matcher.group(0)));
            if (keeperCode != null) {
                return keeperCode.getMsg();
            }
        }
        return message;
    }

    enum KeeperCode {
        COMMON_FAIL(-1, "common exception"),
        SUCCESS(0, "success"),
        DEFAULT_SUCCESS(200, "success"),
        TABLE_NOT_EXISTED(6000, "table is not exist."),

        /** http call exception */
        REQUEST_FAILED(90001, "http call exception"),
        /** json parse error */
        PARSE_FAILED(90002, "json parse error");

        private final int code;
        private final String msg;

        KeeperCode(Integer code, String msg) {
            this.code = code;
            this.msg = msg;
        }

        public static KeeperCode fromCode(Integer code) {
            for (KeeperCode keeperCode : KeeperCode.values()) {
                if (keeperCode.getCode().equals(code)) {
                    return keeperCode;
                }
            }
            return null;
        }

        public Integer getCode() {
            return code;
        }

        public String getMsg() {
            return msg;
        }
    }

    @Override
    public String toString() {
        return "KeeperMessage{"
                + "code="
                + code
                + ", message='"
                + message
                + '\''
                + ", traceId='"
                + traceId
                + '\''
                + ", data="
                + data
                + '}';
    }

    public static final class KeeperMessageBuilder<T> {
        private static final EnumSet<KeeperCode> SUCCESS_CODE =
                EnumSet.of(KeeperCode.SUCCESS, KeeperCode.DEFAULT_SUCCESS);
        private Integer code;
        private String message;
        private String traceId;
        private T data;

        private <T> KeeperMessageBuilder() {}

        public static <T> KeeperMessageBuilder<T> anKeeperMessage() {
            return new KeeperMessageBuilder<T>();
        }

        public KeeperMessageBuilder<T> withCode(Integer code) {
            this.code = code;
            return this;
        }

        public KeeperMessageBuilder<T> withMessage(String message) {
            this.message = message;
            return this;
        }

        public KeeperMessageBuilder<T> withTraceId(String traceId) {
            this.traceId = traceId;
            return this;
        }

        public KeeperMessageBuilder<T> withData(T data) {
            this.data = data;
            return this;
        }

        public KeeperMessage<T> build() {
            KeeperMessage<T> KeeperMessage = new KeeperMessage<T>();
            KeeperMessage.setCode(code);
            KeeperMessage.setMessage(message);
            KeeperMessage.setTraceId(traceId);
            KeeperMessage.setData(data);
            return KeeperMessage;
        }
    }
}
