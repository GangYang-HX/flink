package org.apache.flink.bili.external.archer;

import java.util.EnumSet;
import java.util.stream.Collectors;

/** archer base response message */
public class ArcherMessage<T> {

    private Integer code;

    private String message;

    private String traceId;

    private T data;

    private static final EnumSet<ArcherCode> SUCCESS_CODE =
            EnumSet.of(ArcherCode.SUCCESS, ArcherCode.DEFAULT_SUCCESS);


	public static <T> ArcherMessage<T> requestFailedResponse() {
		return ArcherMessageBuilder.<T>anArcherMessage().withCode(ArcherCode.REQUEST_FAILED.getCode()).build();

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

	public static <T> boolean isSuccessful(ArcherMessage<T> archerMessage) {
        if (archerMessage == null) {
            return false;
        }

        return SUCCESS_CODE.stream()
                .map(ArcherCode::getCode)
                .collect(Collectors.toList())
                .contains(archerMessage.getCode());
    }

    enum ArcherCode {
        SUCCESS(0),
        DEFAULT_SUCCESS(200),
        CANCELLED(1),
        UNKNOWN(2),
        INVALID_ARGUMENT(3),
        DEADLINE_EXCEEDED(4),
        NOT_FOUND(5),
        ALREADY_EXISTS(6),
        PERMISSION_DENIED(7),
        RESOURCE_EXHAUSTED(8),
        FAILED_PRECONDITION(9),
        ABORTED(10),
        OUT_OF_RANGE(11),
        UNIMPLEMENTED(12),
        INTERNAL(13),
        UNAVAILABLE(14),
        DATA_LOSS(15),
        UNAUTHENTICATED(16),

        /** Http请求失败 */
        REQUEST_FAILED(90001),
        /** json解析错误 */
        PARSE_FAILED(90002);

        private final Integer code;

        ArcherCode(Integer code) {
            this.code = code;
        }

		public Integer getCode() {
			return code;
		}
	}

	@Override
	public String toString() {
		return "ArcherMessage{" +
			"code=" + code +
			", message='" + message + '\'' +
			", traceId='" + traceId + '\'' +
			", data=" + data +
			'}';
	}

	public static final class ArcherMessageBuilder<T> {
		private static final EnumSet<ArcherCode> SUCCESS_CODE =
				EnumSet.of(ArcherCode.SUCCESS, ArcherCode.DEFAULT_SUCCESS);
		private Integer code;
		private String message;
		private String traceId;
		private T data;

		private <T>ArcherMessageBuilder() {
		}

		public static <T> ArcherMessageBuilder<T> anArcherMessage() {
			return new ArcherMessageBuilder<T>();
		}

		public ArcherMessageBuilder<T> withCode(Integer code) {
			this.code = code;
			return this;
		}

		public ArcherMessageBuilder<T> withMessage(String message) {
			this.message = message;
			return this;
		}

		public ArcherMessageBuilder<T> withTraceId(String traceId) {
			this.traceId = traceId;
			return this;
		}

		public ArcherMessageBuilder<T> withData(T data) {
			this.data = data;
			return this;
		}

		public ArcherMessage<T> build() {
			ArcherMessage<T> archerMessage = new ArcherMessage<T>();
			archerMessage.setCode(code);
			archerMessage.setMessage(message);
			archerMessage.setTraceId(traceId);
			archerMessage.setData(data);
			return archerMessage;
		}
	}

}
