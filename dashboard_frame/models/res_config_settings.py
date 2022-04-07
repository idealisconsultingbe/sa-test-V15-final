from odoo import models, fields


class ResConfigSettings(models.TransientModel):
    _inherit = 'res.config.settings'

    module_import_dash_accounting = fields.Boolean(string='Import Dashboard Accounting')
    module_import_dash_crm = fields.Boolean(string='Import Dashboard CRM')
    module_import_dash_hr = fields.Boolean(string='Import Dashboard HR')
    module_import_dash_sales = fields.Boolean(string='Import Dashboard Sales')
