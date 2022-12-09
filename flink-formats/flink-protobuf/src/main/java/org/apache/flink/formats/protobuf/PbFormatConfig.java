package org.apache.flink.formats.protobuf;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.formats.protobuf.PbFormatOptions.*;

/** Config of protobuf configs. */
public class PbFormatConfig implements Serializable {
    private String messageClassName;
    private boolean ignoreParseErrors;
    private boolean readDefaultValues;
    private String writeNullStringLiterals;
	private boolean ignoreNullRows;
	private boolean addDefaultValue;

	public PbFormatConfig(
		String messageClassName,
		boolean ignoreParseErrors,
		boolean readDefaultValues,
		String writeNullStringLiterals,
		boolean ignoreNullRows,
	    boolean addDefaultValue) {
		this.messageClassName = messageClassName;
		this.ignoreParseErrors = ignoreParseErrors;
		this.readDefaultValues = readDefaultValues;
		this.writeNullStringLiterals = writeNullStringLiterals;
		this.ignoreNullRows = ignoreNullRows;
		this.addDefaultValue = addDefaultValue;

	}

	public String getMessageClassName() {
		return messageClassName;
	}

	public boolean isIgnoreParseErrors() {
		return ignoreParseErrors;
	}

	public boolean isReadDefaultValues() {
		return readDefaultValues;
	}

	public String getWriteNullStringLiterals() {
		return writeNullStringLiterals;
	}

	public boolean isIgnoreNullRows() {
		return ignoreNullRows;
	}

	public boolean isAddDefaultValue() {
		return addDefaultValue;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		PbFormatConfig that = (PbFormatConfig) o;
		return ignoreParseErrors == that.ignoreParseErrors &&
			readDefaultValues == that.readDefaultValues &&
			ignoreNullRows == that.ignoreNullRows &&
			addDefaultValue == that.addDefaultValue &&
			Objects.equals(messageClassName, that.messageClassName) &&
			Objects.equals(writeNullStringLiterals, that.writeNullStringLiterals);
	}

	@Override
    public int hashCode() {
		return Objects.hash(
			messageClassName, ignoreParseErrors, readDefaultValues, writeNullStringLiterals, ignoreNullRows, addDefaultValue);
    }

	/** Builder of PbFormatConfig. */
	public static final class PbFormatConfigBuilder {
		private String messageClassName;
		private boolean ignoreParseErrors = IGNORE_PARSE_ERRORS.defaultValue();
		private boolean readDefaultValues = READ_DEFAULT_VALUES.defaultValue();
		private String writeNullStringLiterals = WRITE_NULL_STRING_LITERAL.defaultValue();
		private boolean ignoreNullRows = IGNORE_NULL_ROWS.defaultValue();
		private boolean addDefaultValue = ADD_DEFAULT_VALUE.defaultValue();

		public PbFormatConfigBuilder() {
		}

		public static PbFormatConfigBuilder aPbFormatConfig() {
			return new PbFormatConfigBuilder();
		}

		public PbFormatConfigBuilder messageClassName(String messageClassName) {
			this.messageClassName = messageClassName;
			return this;
		}

		public PbFormatConfigBuilder ignoreParseErrors(boolean ignoreParseErrors) {
			this.ignoreParseErrors = ignoreParseErrors;
			return this;
		}

		public PbFormatConfigBuilder readDefaultValues(boolean readDefaultValues) {
			this.readDefaultValues = readDefaultValues;
			return this;
		}

		public PbFormatConfigBuilder writeNullStringLiterals(String writeNullStringLiterals) {
			this.writeNullStringLiterals = writeNullStringLiterals;
			return this;
		}

		public PbFormatConfigBuilder ignoreNullRows(boolean ignoreNullRows) {
			this.ignoreNullRows = ignoreNullRows;
			return this;
		}

		public PbFormatConfigBuilder addDefaultValue(boolean addDefaultValue) {
			this.addDefaultValue = addDefaultValue;
			return this;
		}

		public PbFormatConfig build() {
			return new PbFormatConfig(messageClassName, ignoreParseErrors, readDefaultValues, writeNullStringLiterals, ignoreNullRows, addDefaultValue);
		}
	}


}
