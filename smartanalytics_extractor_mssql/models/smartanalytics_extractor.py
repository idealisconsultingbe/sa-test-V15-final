import datetime
import pymssql

from odoo import fields, models, _
from odoo.exceptions import ValidationError


class SmartanalyticsExtractorBackend(models.Model):
    _inherit = 'smartanalytics.extractor.backend'

    type = fields.Selection(selection_add=[('mssql', 'MsSQL')])
    mssql_port = fields.Integer(string="MsSQL Port", default="1433")
    mssql_server = fields.Char(string="MsSQL Server")
    mssql_database = fields.Char(string="MsSQL Database")
    mssql_user = fields.Char(string="MsSQL User")
    mssql_password = fields.Char(string="MsSQL Password")

    def _default_python_code(self):
        res = super()._default_python_code()
        return res + """# Available variables:
#  - mssql_port
#  - mssql_server
#  - mssql_database
#  - mssql_user
#  - mssql_password
#
# Create a connection using this line of code:
# cnx = pyodbc.connect(port=mssql_port, server=mssql_server, database=mssql_database, user=mssql_user, password=mssql_password)
# Get a cursor with this line:
# cursor = cnx.cursor()
# Don't forget to commit, close the cursor and close the connection:
# cnx.commit()
# cursor.close()
# cnx.close()
#
\n\n\n"""

    def test_connection(self):
        self.ensure_one()
        if self.type == 'mssql':
            try:
                cnx = self._get_mssql_connection()
                try:
                    cursor = cnx.cursor()
                except Exception:
                    raise ValidationError(_('Error while getting cursor from to MsSQL connection'))
                else:
                    cursor.close()
            except Exception:
                raise ValidationError(_('Error while connecting to MsSQL Database'))
            else:
                cnx.close()
        else:
            super().test_connection()

    def _get_mssql_connection(self):
        self.ensure_one()
        cnx = pymssql.connect(port=self.mssql_port,
                              server=self.mssql_server,
                              database=self.mssql_database,
                              user=self.mssql_user,
                              password=self.mssql_password,
                              )
        return cnx

    def _get_eval_context(self):
        eval_context = super()._get_eval_context()
        eval_context.update({
            'mssql_port': self.mssql_port,
            'mssql_server': self.mssql_server,
            'mssql_database': self.mssql_database,
            'mssql_user': self.mssql_user,
            'mssql_password': self.mssql_password,
        })
        return eval_context


class SmartanalyticsExtractorExtract(models.Model):
    _inherit = 'smartanalytics.extractor.extract'

    def action_run_import(self):
        res = super().action_run_import()
        for record in self:
            if record.type == 'mssql':
                try:
                    cnx = record.backend_id._get_mssql_connection()
                    cursor = cnx.cursor()
                    record._mssql_drop_table(cursor)
                    record._mssql_create_table(cursor)
                    record._mssql_insert_into_table(cursor)
                    cnx.commit()
                    record.log = 'Import finished successfully !'
                    record.state = 'succeed'
                except Exception as error:
                    errors = f'Import failed !!\n\nErrors:\n{error}'
                    record.log = errors
                    record.state = 'failed'
                else:
                    cursor.close()
                    cnx.close()
        return res

    def _mssql_drop_table(self, cursor):
        self.ensure_one()
        query = f"DROP TABLE IF EXISTS {self.table};"
        cursor.execute(query)

    def _mssql_create_table(self, cursor):
        self.ensure_one()
        fields = ', '.join(self._mssql_get_table_fields())
        query = f"CREATE TABLE {self.table} ({fields});"
        cursor.execute(query)

    def _mssql_get_table_fields(self):
        self.ensure_one()
        type_mapping = {
            'NUMERIC': 'NUMERIC',
            'BOOL': 'BOOL',
            'STRING': 'STRING',
        }
        fields = []
        for field in self.field_ids:
            field_type = type_mapping.get(field.dwh_type, field.dwh_type)
            field_required = 'NOT NULL' if field.dwh_required else ''
            declaration = f"{field.dwh_name} {field_type} {field_required}"
            fields.append(declaration)
        return fields

    def _mssql_insert_into_table(self, cursor):
        self.ensure_one()

        column_names = dict([(field.column, field.dwh_name) for field in self.field_ids])
        column_types = dict([(field.column, field.dwh_type) for field in self.field_ids])
        columns = self._get_columns_from_query()
        fields = ', '.join([column_names[column] for column in columns])
        placeholders = ', '.join(['%s' for f in columns])
        query = f"INSERT INTO {self.table} ({fields}) VALUES ({placeholders});"
        print(query)

        self.env.cr.execute(self.query)

        for row in self.env.cr.fetchall():
            print(row)
            for i, column in enumerate(columns):
                value = row[i]
                print(value)
            cursor.execute(query, value)
