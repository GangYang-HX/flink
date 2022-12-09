/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.sql.parser.ddl;

import org.apache.calcite.sql.*;
import org.apache.calcite.util.NlsString;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Table column of a CREATE TABLE DDL.
 */
public class SqlTableColumn extends SqlCall {
	private static final SqlSpecialOperator OPERATOR =
		new SqlSpecialOperator("COLUMN_DECL", SqlKind.COLUMN_DECL);

	private SqlIdentifier name;
	private SqlDataTypeSpec type;

	private SqlTableConstraint constraint;

	private SqlCharStringLiteral comment;

	public SqlTableColumn(SqlIdentifier name,
			SqlDataTypeSpec type,
			@Nullable SqlTableConstraint constraint,
			@Nullable SqlCharStringLiteral comment,
			SqlParserPos pos) {
		super(pos);
		this.name = requireNonNull(name, "Column name should not be null");
		this.type = requireNonNull(type, "Column type should not be null");
		this.constraint = constraint;
		this.comment = comment;
	}

	@Override
	public SqlOperator getOperator() {
		return OPERATOR;
	}

	@Override
	public List<SqlNode> getOperandList() {
		return ImmutableNullableList.of(name, type, comment);
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		this.name.unparse(writer, leftPrec, rightPrec);
		writer.print(" ");
		this.type.unparse(writer, leftPrec, rightPrec);
		if (!this.type.getNullable()) {
			// Default is nullable.
			writer.keyword("NOT NULL");
		}
		if (this.constraint != null) {
			this.constraint.unparse(writer, leftPrec, rightPrec);
		}
		if (this.comment != null) {
			writer.print(" COMMENT ");
			this.comment.unparse(writer, leftPrec, rightPrec);
		}
	}

	public SqlIdentifier getName() {
		return name;
	}

	public void setName(SqlIdentifier name) {
		this.name = name;
	}

	public SqlDataTypeSpec getType() {
		return type;
	}

	public void setType(SqlDataTypeSpec type) {
		this.type = type;
	}

	public Optional<SqlTableConstraint> getConstraint() {
		return Optional.ofNullable(constraint);
	}

	public Optional<SqlCharStringLiteral> getComment() {
		return Optional.ofNullable(comment);
	}

	public void setComment(SqlCharStringLiteral comment) {
		this.comment = comment;
	}


	public static class SqlMetadataColumn extends SqlTableColumn {

		private final @Nullable SqlNode metadataAlias;

		private final boolean isVirtual;

		public SqlMetadataColumn(SqlIdentifier name, SqlDataTypeSpec type,
								 @Nullable SqlTableConstraint constraint,
								 @Nullable SqlCharStringLiteral comment, SqlParserPos pos,
								 @Nullable SqlNode metadataAlias, boolean isVirtual) {
			super(name, type, constraint, comment, pos);
			this.metadataAlias = metadataAlias;
			this.isVirtual = isVirtual;
		}

		@Override
		public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
			this.getName().unparse(writer, leftPrec, rightPrec);
			writer.print(" ");
			this.getType().unparse(writer, leftPrec, rightPrec);
			if (!this.getType().getNullable()) {
				// Default is nullable.
				writer.keyword("NOT NULL");
			}
			writer.keyword("METADATA");
			if (metadataAlias != null) {
				writer.keyword("FROM");
				metadataAlias.unparse(writer, leftPrec, rightPrec);
			}
			if (isVirtual) {
				writer.keyword("VIRTUAL");
			}
			if (this.getComment().orElse(null) != null) {
				writer.print(" COMMENT ");
				this.getComment().get().unparse(writer, leftPrec, rightPrec);
			}
		}

		public Optional<String> getMetadataAlias() {
			return Optional.ofNullable(metadataAlias)
				.map(alias -> ((NlsString) SqlLiteral.value(alias)).getValue());
		}

		public boolean isVirtual() {
			return isVirtual;
		}

		@Override
		public @Nonnull
		List<SqlNode> getOperandList() {
			return ImmutableNullableList.of(super.getName(), super.getType(), super.getComment().orElse(null));
		}
	}
}
