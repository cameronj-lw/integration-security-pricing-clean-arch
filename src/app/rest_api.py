
# core python
import argparse
from configparser import ConfigParser
import datetime
import logging
import os
import socket
import sys
import time

# pypi
from flask import Flask

# Append to pythonpath
src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(src_dir)

# native
from application.query_handlers import (
    PriceFeedWithStatusQueryHandler, PriceAuditReasonQueryHandler,
    ManualPricingSecurityQueryHandler, UserWithColumnConfigQueryHandler,
    PricingAttachmentByDateQueryHandler, PriceCountBySourceQueryHandler,
    HeldSecurityPriceQueryHandler, PriceAuditEntryQueryHandler
)
from application.command_handlers import (
    SecurityCommandHandler, 
    UserWithColumnConfigCommandHandler, PricingAttachmentByDateCommandHandler,
    PriceByIMEXCommandHandler, PriceAuditEntryCommandHandler
)
from infrastructure.sql_repositories import (
    MGMTDBPriceFeedWithStatusRepository, CoreDBManualPricingSecurityRepository,
    CoreDBColumnConfigRepository, CoreDBSecurityRepository, LWDBPriceRepository,
    CoreDBPriceAuditEntryRepository
)
from infrastructure.file_repositories import (
    DataDirDateWithPricingAttachmentsRepository, JSONHeldSecuritiesWithPricesRepository
)
from infrastructure.api_repositories import APXPriceRepository
from interface.routes import blueprint  # import routes
from infrastructure.util.config import AppConfig
from infrastructure.util.file import prepare_dated_file_path
from infrastructure.util.logging import setup_logging



# Initialize the Flask app and register blueprint
app = Flask(__name__)

# Initialize command handlers and query handlers
price_feed_with_status_query_handler = PriceFeedWithStatusQueryHandler(MGMTDBPriceFeedWithStatusRepository())
manual_pricing_security_command_handler = SecurityCommandHandler(CoreDBManualPricingSecurityRepository())
manual_pricing_security_query_handler = ManualPricingSecurityQueryHandler(CoreDBManualPricingSecurityRepository())
column_config_command_handler = UserWithColumnConfigCommandHandler(CoreDBColumnConfigRepository())
column_config_query_handler = UserWithColumnConfigQueryHandler(CoreDBColumnConfigRepository())
price_audit_reason_query_handler = PriceAuditReasonQueryHandler()
pricing_attachment_by_date_command_handler = PricingAttachmentByDateCommandHandler(DataDirDateWithPricingAttachmentsRepository())
pricing_attachment_by_date_query_handler = PricingAttachmentByDateQueryHandler(DataDirDateWithPricingAttachmentsRepository())
counts_by_source_query_handler = PriceCountBySourceQueryHandler(JSONHeldSecuritiesWithPricesRepository())
held_security_price_query_handler = HeldSecurityPriceQueryHandler(JSONHeldSecuritiesWithPricesRepository())
price_by_imex_command_handler = PriceByIMEXCommandHandler(CoreDBSecurityRepository(), [LWDBPriceRepository(), APXPriceRepository()])
# TODO_GOLIVE: add Price repo to above for manual_price table
audit_trail_command_handler = PriceAuditEntryCommandHandler(CoreDBPriceAuditEntryRepository())
audit_trail_query_handler = PriceAuditEntryQueryHandler(CoreDBPriceAuditEntryRepository())

# Inject dependencies into the Flask app context
app.config['feed_status_query_handler'] = price_feed_with_status_query_handler
app.config['manual_pricing_security_command_handler'] = manual_pricing_security_command_handler
app.config['manual_pricing_security_query_handler'] = manual_pricing_security_query_handler
app.config['column_config_command_handler'] = column_config_command_handler
app.config['column_config_query_handler'] = column_config_query_handler
app.config['audit_reason_query_handler'] = price_audit_reason_query_handler
app.config['pricing_attachment_by_date_command_handler'] = pricing_attachment_by_date_command_handler
app.config['pricing_attachment_by_date_query_handler'] = pricing_attachment_by_date_query_handler
app.config['counts_by_source_query_handler'] = counts_by_source_query_handler
app.config['held_security_price_query_handler'] = held_security_price_query_handler
app.config['price_by_imex_command_handler'] = price_by_imex_command_handler
app.config['audit_trail_query_handler'] = audit_trail_query_handler
app.config['audit_trail_command_handler'] = audit_trail_command_handler
# app.config['price_feed_service'] = price_feed_service

# Register the blueprint with the app, passing the app's config
app.register_blueprint(blueprint, config=app.config)


if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser(description='Kafka Consumer')
        parser.add_argument('--log_level', '-l', type=str.upper, choices=['DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL'], help='Log level')
        args = parser.parse_args()
        # Looks like flask runs 2 (or more?) python processes when running the flask app.
        # This caused "file is being used by another process" when trying to rotate out an old log file.
        # Solution is to use rotate=False as follows.
        log_file = prepare_dated_file_path(AppConfig().parser.get("logging", "log_dir"), datetime.date.today(), AppConfig().parser.get("logging", "rest_api_logfile"), rotate=False)
        setup_logging(args.log_level, log_file)

        # Get configs and run flask app
        host = AppConfig().parser.get("rest_api", "host")
        port = AppConfig().parser.get("rest_api", "port")
        debug = AppConfig().parser.get("rest_api", "debug")
        app.run(host=host, port=port, debug=debug, threaded=False, processes=1)
    except Exception as e:
        logging.exception(f"{type(e).__name__}: {e}")
        sys.exit(1)
    sys.exit(0)


