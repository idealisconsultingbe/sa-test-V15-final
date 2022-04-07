# -*- coding: utf-8 -*-
{
    'name': "import_dash_crm",

    'author': "Idealis Consulting",
    'website': "http://www.idealisconsulting.com",

    # Categories can be used to filter modules in modules listing
    # Check https://github.com/odoo/odoo/blob/master/odoo/addons/base/module/module_data.xml
    # for the full list
    'category': 'Uncategorized',
    'version': '0.1',

    # any module necessary for this one to work correctly
    'depends': ['base', 'crm', 'dashboard_frame'],

    # always loaded
    'data': [
        # 'security/ir.model.access.csv',
        'data/smart.analytics.dashboard.csv',
        'data/smartanalytics.extractor.backend.csv',
        'data/smartanalytics.extractor.extract.csv',
    ],
    'auto-install': False,

}
