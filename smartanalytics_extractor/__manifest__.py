{
    'name': 'Smart Analytics - Extractor',
    'summary': 'Extract data to a datawarehouse',
    'description': 'This module is the base module to extract data to a datawarehouse. It must be used with bigquery or mysql.',
    'version': '1.1',
    'category': 'Other',
    'author': 'Idealis Consulting',
    'website': 'https://idealisconsulting.com/',
    'depends': ['base'],
    'data': [
        'data/ir_cron.xml',
        'security/res_groups.xml',
        'security/ir.model.access.csv',
        'views/smartanalytics_extractor.xml',
    ],
    'installable': True,
}
