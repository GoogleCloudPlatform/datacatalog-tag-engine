# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.
#

from dataclasses import dataclass
from typing import Optional
from typing import Tuple
from typing import List

from sql_translator.rfmt.blocks import LineBlock as LB
from sql_translator.rfmt.blocks import TextBlock as TB
from sql_translator.rfmt.blocks import StackBlock as SB
from sql_translator.rfmt.blocks import IndentBlock as IB
from sql_translator.rfmt.blocks import ChoiceBlock as CB
from sql_translator.rfmt.blocks import WrapBlock as WB

from sql_translator.sql_parser.utils import with_commas

from sql_translator.sql_parser.types import SQLType
from sql_translator.sql_parser.query import SQLQuery
from sql_translator.sql_parser.query import SQLNamedTable
from sql_translator.sql_parser.query_impl import SQLFrom
from sql_translator.sql_parser.expr import SQLExpr
from sql_translator.sql_parser.ident import SQLIdentifierPath
from sql_translator.sql_parser.node import SQLNode
from sql_translator.sql_parser.node import SQLNodeList
from sql_translator.sql_parser.query import SQLTableSource


@dataclass(frozen=True)
class SQLDML(SQLNode):

    @staticmethod
    def consume(lex):
        return (SQLInsert.consume(lex) or
                SQLUpdate.consume(lex) or
                SQLDelete.consume(lex) or
                SQLCreate.consume(lex))


@dataclass(frozen=True)
class SQLInsert(SQLDML):
    table: SQLTableSource
    fields: Optional[SQLNodeList[SQLNode]]
    sql: SQLNode

    def sqlf(self, compact):
        # TODO(scannell) Add compact version
        if not self.fields:
            return SB([
                LB([
                    TB('INSERT INTO '),
                    self.table.sqlf(compact=True),
                    TB(' ('),
                ]),
                IB(self.sql.sqlf(compact)),
                TB(')'),
            ])
        return SB([
            LB([
                TB('INSERT INTO '),
                self.table.sqlf(compact=True),
                TB(' ('),
            ]),
            IB(
                SB(with_commas(compact, self.fields, tail=')'))
            ),
            self.sql.sqlf(compact)
        ])

    @staticmethod
    def consume(lex) -> 'Optional[SQLInsert]':
        if not lex.consume('INSERT'):
            return None

        lex.consume('INTO')

        table = SQLNamedTable.parse(lex, is_write=True)

        lex.expect('(')

        # It's possible to have a SQL statement in here.
        # .. this is with an implicit column list.
        query = SQLQuery.consume(lex)
        if query:
            lex.expect(')')
            return SQLInsert(table, None, query)

        # Pull out the values
        cols = []
        while True:
            cols.append(SQLIdentifierPath.parse(lex))
            if not lex.consume(','):
                break
        lex.expect(')')

        # Now the SQL
        query = SQLQuery.consume(lex)
        if query:
            return SQLInsert(table, SQLNodeList(cols), query)

        lex.error('Insert with explicit values unsupported')
        return None


@dataclass(frozen=True)
class SQLDelete(SQLDML):
    table: SQLNode
    where_expr: SQLNode

    def sqlf(self, compact):
        delete = [TB('DELETE FROM '), self.table.sqlf(compact)]
        return SB([
            LB(delete),
            LB([
                TB('WHERE '),
                self.where_expr.sqlf(compact),
            ])
        ])

    @staticmethod
    def consume(lex) -> 'Optional[SQLDelete]':
        if not lex.consume('DELETE'):
            return None

        lex.consume('FROM')

        table = SQLNamedTable.parse(lex, is_write=True)

        lex.expect('WHERE')

        where_expr = SQLExpr.parse(lex)

        return SQLDelete(table, where_expr)


@dataclass(frozen=True)
class SQLUpdate(SQLDML):
    table_name: SQLNode
    update_fields: SQLNodeList[SQLIdentifierPath]
    update_exprs: SQLNodeList[SQLNode]
    from_tables: SQLNode
    where_expr: SQLNode

    # TODO(scannell) honour compact mode
    def sqlf(self, compact):
        tname = [TB('UPDATE '), self.table_name.sqlf(compact)]

        updatestmt = [
            LB(tname),
            TB('SET'),
        ]
        for i in range(len(self.update_fields)):
            field = self.update_fields[i]
            expr = self.update_exprs[i]
            if i == len(self.update_fields)-1:
                updatestmt.append(LB([
                    field.sqlf(False), TB(' = '), expr.sqlf(False)
                ]))
            else:
                updatestmt.append(LB([
                    field.sqlf(False), TB(' = '), expr.sqlf(False),
                    TB(',')
                ]))
        if self.from_tables:
            updatestmt.append(LB([
                TB(' FROM '),
                self.from_tables.sqlf(False)
            ]))
        if self.where_expr:
            updatestmt.append(LB([
                TB(' WHERE '),
                self.where_expr.sqlf(False)
            ]))
        return SB(updatestmt)

    @staticmethod
    def consume(lex) -> 'Optional[SQLUpdate]':
        if not lex.consume('UPDATE'):
            return None

        table_name = SQLNamedTable.parse(lex, is_write=True)

        lex.expect('SET')

        update_fields = []
        update_exprs = []
        while True:
            update_fields.append(SQLIdentifierPath.parse(lex))
            lex.expect('=')
            update_exprs.append(SQLExpr.parse(lex))
            if not lex.consume(','):
                break

        from_tables = lex.consume('FROM') and SQLFrom.parse(lex)
        where_expr = lex.consume('WHERE') and SQLExpr.parse(lex)

        return SQLUpdate(table_name,
                         SQLNodeList(update_fields),
                         SQLNodeList(update_exprs),
                         from_tables, where_expr)


@dataclass(frozen=True)
class SQLOption(SQLDML):
    key: str
    val: SQLExpr

    def sqlf(self, compact):
        compact_sql = LB([TB(key), TB('='), TB(self.val.sqlf(True))])
        if compact:
            return compact_sql

        return CB([
            compact_sql,
            SB([
                LB([TB(key), TB(' =')]),
                IB(self.val.sqlf(False)),
            ])
        ])

    @staticmethod
    def parse(lex) -> 'SQLOption':
        key = lex.consumer_identifier()
        lex.expect('=')
        val = SQLExpr.parse(lex)

        return SQLOption(key, val)


@dataclass(frozen=True)
class SQLOptions(SQLDML):
    options: SQLNodeList[SQLOption]

    def sqlf(self, compact):
        lines = [TB('OPTIONS(')]
        lines.extend(with_commas(True, self.options))
        lines.append(TB(')'))

        if compact:
            return LB(lines)

        stack = [TB('OPTIONS(')]
        stack.extend(IB(WB(with_commas(False, self.options))))
        stack.append(TB(')'))

        return CB([
            LB(lines),
            SB(stack),
        ])

    @staticmethod
    def consume(lex) -> 'Optional[SQLOptions]':
        if not lex.consume('OPTIONS'):
            return None

        lex.expect('(')
        options: List[SQLOption] = []
        while True:
            options.append(SQLOption.parse(lex))
            if not lex.consume(','):
                break
        lex.expect(')')

        return SQLOptions(SQLNodeList(options))


@dataclass(frozen=True)
class SQLColumn(SQLDML):
    name: str
    col_type: SQLType
    nullable: bool
    opts: Optional[SQLOptions]

    def sqlf(self, compact):
        lines = [TB(self.name), TB(' '),
                 self.col_type.sqlf(True)]
        if not self.nullable:
            lines.append(TB(' NOT NULL'))
        if self.opts:
            lines.append(TB(' '))
            lines.append(self.opts.sqlf(True))

        if compact:
            return LB(lines)

        # Indented block for non-compact
        iblock = [self.col_type.sqlf(False)]
        if not self.nullable:
            iblock.append(TB('NOT NULL'))
        if self.opts:
            iblock.append(self.opts.sqlf(False))

        return CB([
            LB(lines),
            SB([
                TB(self.name),
                IB(SB(iblock)),
            ])
        ])

    @staticmethod
    def parse(lex) -> 'SQLColumn':
        name = (lex.consume_identifier() or
                lex.error('expected column name'))
        col_type = SQLType.parse(lex)

        # Get whether nullable
        nullable = True
        if lex.consume('NOT'):
            lex.expect('NULL')
            nullable = False

        # Any options
        opts = SQLOptions.consume(lex)

        return SQLColumn(
            name, col_type,
            nullable, opts
        )


@dataclass(frozen=True)
class SQLCreate(SQLDML):
    clause: str
    table: SQLTableSource
    columns: SQLNodeList[SQLColumn]
    options: Optional[SQLOptions]
    query: Optional[SQLNode]

    def sqlf(self, compact):
        lines = [TB(self.clause), TB(' ')]
        lines.append(self.table.sqlf(True))
        if self.columns:
            lines.append(TB(' ('))
            lines.extend(with_commas(True, self.columns))
            lines.append(TB(')'))
        if self.options:
            lines.append(TB(' '))
            lines.append(self.options.sqlf(True))
        if self.query:
            lines.append(TB(' AS '))
            lines.append(self.query.sqlf(True))

        if compact:
            return LB(lines)

        stack = [LB([TB(self.clause),TB(' '),self.table.sqlf(True)])]
        if self.columns:
            stack.extend([
                TB('('),
                IB(WB(
                    with_commas(False, self.columns),
                )),
                TB(')'),
            ])
        if self.options:
            stack.append(self.options.sqlf(False))
        if self.query:
            stack.extend([
                TB('AS'),
                self.query.sqlf(False),
            ])

        return CB([
            LB(lines),
            SB(stack),
        ])

    @staticmethod
    def consume(lex):
        if not lex.consume('CREATE'):
            return None

        #  Find CFREATE OR REPLACE TABLE clause
        clause = None
        if lex.consume('OR'):
            lex.expect('REPLACE')
            lex.expect('TABLE')
            clause = 'CREATE OR REPLACE TABLE'
        else:
            lex.expect('TABLE')

            # Check for IF NOT EXISTS
            if lex.consume('IF'):
                lex.expect('NOT')
                lex.expect('EXISTS')
                clause = 'CREATE TABLE IF NOT EXISTS'
            else:
                clause = 'CREATE TABLE'

        # Parse table
        table = SQLNamedTable.parse_no_alias(lex, is_write=True)

        # Parse columns
        columns: List[SQLColumn] = []
        if lex.consume('('):
            while True:
                columns.append(SQLColumn.parse(lex))
                if not lex.consume(','):
                    break
            lex.expect(')')

        partition_by = None
        if lex.consume('PARTITION'):
            lex.expect('BY')
            # TODO: Finish
            lex.error('PARTITION BY unimplemented')

        cluster_by = None
        if lex.consume('CLUSTER'):
            lex.expect('BY')
            # TODO: Finish
            lex.error('CLUSTER BY unimplemented')

        # Find options
        options = SQLOptions.consume(lex)

        # Find query-for-creation (if any)
        query = None
        if lex.consume('AS'):
            query = SQLQuery.parse(lex)

        return SQLCreate(clause, table, columns, options, query)


