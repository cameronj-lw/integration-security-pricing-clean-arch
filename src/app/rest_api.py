
# core python
from configparser import ConfigParser
import logging
import os
import sys

# pypi
from flask import Flask

# native
from application.query_handlers import (
    PriceFeedWithStatusQueryHandler, PriceAuditReasonQueryHandler,
    ManualPricingSecurityQueryHandler
)
from application.command_handlers import (
    PriceFeedCommandHandler, SecurityCommandHandler
)
from infrastructure.util.config import AppConfig
from infrastructure.sql_repositories import (
    MGMTDBPriceFeedWithStatusRepository, CoreDBManualPricingSecurityRepository
)
from interface.routes import blueprint  # import routes



# Initialize the Flask app and register blueprint
app = Flask(__name__)


# Initialize query handlers
price_feed_with_status_query_handler = PriceFeedWithStatusQueryHandler(MGMTDBPriceFeedWithStatusRepository())
manual_pricing_security_command_handler = SecurityCommandHandler(CoreDBManualPricingSecurityRepository())
manual_pricing_security_query_handler = ManualPricingSecurityQueryHandler(CoreDBManualPricingSecurityRepository())
price_audit_reason_query_handler = PriceAuditReasonQueryHandler()

# Initialize services
# price_feed_service = PriceFeedService()

# Inject dependencies into the Flask app context
app.config['feed_status_query_handler'] = price_feed_with_status_query_handler
app.config['manual_pricing_security_command_handler'] = manual_pricing_security_command_handler
app.config['manual_pricing_security_query_handler'] = manual_pricing_security_query_handler
app.config['audit_reason_query_handler'] = price_audit_reason_query_handler
# app.config['price_feed_service'] = price_feed_service

# Register the blueprint with the app, passing the app's config
app.register_blueprint(blueprint, config=app.config)


if __name__ == '__main__':
    try:
        host = AppConfig().get("rest_api", "host")
        port = AppConfig().get("rest_api", "port")
        debug = AppConfig().get("rest_api", "debug")
        app.run(host=host, port=port, debug=debug)
    except Exception as e:
        logging.exception(f"{type(e).__name__}: {e}")
        sys.exit(1)
    sys.exit(0)


