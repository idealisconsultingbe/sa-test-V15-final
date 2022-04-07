import datetime
import re

from odoo import api, fields, models, _
from odoo.exceptions import ValidationError
from odoo.tools.safe_eval import test_expr, _SAFE_OPCODES, to_opcodes
from odoo.tools.misc import ustr


def _check_python_code(code):
    if code:
        try:
            OPCODES = _SAFE_OPCODES.union(to_opcodes(['IMPORT_NAME', 'IMPORT_FROM']))
            test_expr(code.strip(), OPCODES, mode='exec')
        except (SyntaxError, TypeError, ValueError) as err:
            if len(err.args) >= 2 and len(err.args[1]) >= 4:
                error = {
                    'message': err.args[0],
                    'filename': err.args[1][0],
                    'lineno': err.args[1][1],
                    'offset': err.args[1][2],
                    'error_line': err.args[1][3],
                }
                msg = "%s : %s at line %d\n%s" % (
                type(err).__name__, error['message'], error['lineno'], error['error_line'])
            else:
                msg = ustr(err)
            raise ValidationError(msg)


class SmartanalyticsExtractorBackend(models.Model):
    _name = 'smartanalytics.extractor.backend'
    _description = 'Smart Analytics Extractor backend'

    name = fields.Char(string='Name', required=True)
    extract_ids = fields.One2many('smartanalytics.extractor.extract', 'backend_id', string='Extracts')
    state = fields.Selection(
        selection=[('new', 'New'), ('succeed', 'Succeed'), ('failed', 'Failed')],
        string='State',
        compute='_compute_state',
        default='new',
    )
    type = fields.Selection(selection=[], string='Type')
    post_extract_code = fields.Text(string='Post-extract Code',
                                    help="Write Python code that will be executed after the extract.\n")

    comment_code = fields.Text(default=lambda self: self._default_python_code(), readonly=True)

    def _default_python_code(self):
        return ""

    def _compute_state(self):
        for record in self:
            states = [extract.state for extract in record.extract_ids]
            if 'failed' in states:
                self.state = 'failed'
            elif (['succeed'] * len(states)) == states:
                self.state = 'succeed'
            else:
                self.state = 'new'

    @api.constrains('post_extract_code')
    def _check_post_extract_code(self):
        for record in self.filtered('post_extract_code'):
            _check_python_code(record.post_extract_code)

    def test_connection(self):
        self.ensure_one()
        return True

    def action_run_all_extracts(self):
        for record in self:
            if not record.type:
                raise ValidationError(_('Type field are empty'))
            for extract in record.extract_ids:
                extract.action_run_import()
            # Python script to run after the extract
            if record.post_extract_code:
                exec(record.post_extract_code.strip(), {}, record._get_eval_context())

    def _get_eval_context(self):
        self.ensure_one()
        return {}


class SmartanalyticsExtractorExtract(models.Model):
    _name = 'smartanalytics.extractor.extract'
    _description = 'Smartanalytics Extractor extract'

    name = fields.Char(string='Name', required=True)
    backend_id = fields.Many2one('smartanalytics.extractor.backend', string='Backend', required=True,
                                 ondelete='cascade')
    type = fields.Selection(related='backend_id.type')
    query = fields.Text(string='Query', required=True)
    table = fields.Char(string='Datawarehouse table name', required=True)
    field_ids = fields.One2many('smartanalytics.extractor.extract.field', 'extract_id', string='Schema fields')
    log = fields.Text(string='Last import log', readonly=True)
    state = fields.Selection(
        selection=[('new', 'New'), ('succeed', 'Succeed'), ('failed', 'Failed')],
        string='State',
        readonly=True,
        default='new',
        required=True,
    )

    # post_extract_code = fields.Text(string='Post-extract Code', help="Write Python code that will be executed after the extract.")

    @api.constrains('query', 'field_ids')
    def _check_query_and_shema(self):
        for record in self:
            # Check if query starts with SELECT
            if not record.query.strip().startswith('SELECT '):
                raise ValidationError(_("Queries must be SELECT query"))
            # Prepare fields and run query
            schema_fields = record.field_ids.mapped('column')
            self.env.cr.execute(record.query)
            for column in self.env.cr.description:
                # Check if column (of the query) is in fields
                if column.name not in schema_fields:
                    raise ValidationError(
                        _('The column "%s" of the query is not defined in fields') % column.name
                    )
                schema_fields.remove(column.name)
            # Check if there are fields that are not in query
            if schema_fields:
                raise ValidationError(
                    _('The following fields are not in the query: %s') % ' ,'.join(schema_fields)
                )

    # @api.constrains('post_extract_code')
    # def _check_post_extract_code(self):
    #     for record in self.filtered('post_extract_code'):
    #         _check_python_code(record.post_extract_code)

    @api.onchange('query')
    def on_change_query(self):
        self.ensure_one()
        if self.query and not self.field_ids:
            columns = self._get_columns_from_query()
            res = []
            for i, column in enumerate(columns):
                res.append((0, 0, {'column': column, 'dwh_name': column, 'dwh_type': 'STRING', 'sequence': i}))
            self.field_ids = res

    def _get_columns_from_query(self):
        self.ensure_one()
        query = self.query.lower().strip()
        start = query.find('select') + 7
        end = re.search(r'\sfrom\s', query).start()
        columns = list(
            map(lambda c: c.split(' as ', 1)[1] if ' as ' in c else c, [f.strip() for f in query[start:end].split(',')])
        )
        return columns

    def _prepare_dwh_schema(self):
        self.ensure_one()
        fields_mapping = {}
        for field in self.field_ids:
            fields_mapping[field.column] = field.dwh_get_field()
        result = []
        for column in self._get_columns_from_query():
            result.append(fields_mapping[column])
        return result

    def _dwh_to_named_data(self, row):
        self.ensure_one()
        res = {}
        column_names = dict([(field.column, field.dwh_name) for field in self.field_ids])
        columns = self._get_columns_from_query()
        for i, column in enumerate(columns):
            if isinstance(row[i], datetime.date):
                res[column_names[column]] = row[i].strftime('%Y-%m-%d')
            elif isinstance(row[i], datetime.datetime):
                res[column_names[column]] = row[i].strftime('%Y-%m-%d %H:%M:%S')
            else:
                res[column_names[column]] = row[i]
        return res

    def _prepare_dwh_datas(self):
        self.env.cr.execute(self.query)
        rows_to_insert = list(map(self._dwh_to_named_data, self.env.cr.fetchall()))
        return rows_to_insert

    def action_run_import(self):
        return


class SmartanalyticsExtractorExtractField(models.Model):
    _name = 'smartanalytics.extractor.extract.field'
    _description = 'Smart Analytics Extractor extract field'
    _order = 'extract_id, sequence, id'
    _rec_name = 'column'

    extract_id = fields.Many2one('smartanalytics.extractor.extract', string='Extract', required=True,
                                 ondelete='cascade')
    column = fields.Char(string='Query column', required=True, help='Name of the column, or the "AS" if it\'s named')
    dwh_name = fields.Char(string='DWH field name', required=True)
    dwh_type = fields.Selection(selection='_selection_type', string='DWH field type', required=True)
    dwh_required = fields.Boolean(string='DWH field required')
    sequence = fields.Integer(string='Sequence', default=10)

    @api.model
    def _selection_type(self):
        return [
            ('INT', 'Integer'),
            ('FLOAT', 'Float'),
            ('NUMERIC', 'Numeric'),
            ('BOOL', 'Boolean'),
            ('STRING', 'Text'),
            ('DATE', 'Date'),
            ('TIME', 'Time'),
            ('DATETIME', 'Datetime'),
        ]

    def dwh_get_field(self):
        self.ensure_one()
        return (self.dwh_name, self.dwh_type, self.dwh_required)

    def dwh_get_schema(self):
        schema = []
        for record in self:
            schema.append(record.dwh_get_field())
        return schema
