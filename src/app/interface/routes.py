
# core python
import datetime
import logging

# pypi
from flask import Blueprint, current_app
from flask_cors import CORS
from flask_restx import Api, Resource

# native
# from app.application.query_handlers import PriceFeedWithStatusQueryHandler
from app.interface.formatters import DefaultRESTFormatter


blueprint = Blueprint('blueprint', __name__)
api = Api(blueprint)
CORS(blueprint)





@api.route('/api/pricing/notification-subscription')
class PricingNotificationSubscription(Resource):
    def post(self):
        payload = api.payload
        return  # TODO_WAVE3: implement
    
    def get(self):
        return  # TODO_WAVE3: implement

    def delete(self):
        payload = api.payload
        return  # TODO_WAVE3: implement

@api.route('/api/pricing/feed-status')
class PricingFeedStatus(Resource):
    formatter = DefaultRESTFormatter()

    def get(self):
        try:
            data_date = datetime.date.today()
            # Get query handler, based on app config
            query_handler = current_app.config['feed_status_query_handler']
            # Get feeds' statuses
            feeds_with_statuses = query_handler.handle(data_date)  # query_handler.repo.get(data_date, feeds)
            # Format into dict (desired format for result)
            result_data = {
                fws.feed.name: {
                    'status': fws.status,
                    'asofdate': fws.status_ts.isoformat(),
                    'normal_eta': fws.feed.get_normal_eta(fws.data_date).isoformat(),
                    'security_type': fws.feed.security_type,
                } for fws in feeds_with_statuses
            }
            # Return standard format
            return self.formatter.success_get(result_data)
        except Exception as e:
            return self.formatter.exception(e)        
            
@api.route('/api/pricing/feed-status/<string:price_date>')
class PricingFeedStatusByDate(Resource):
    formatter = DefaultRESTFormatter()

    def get(self, price_date):
        try:
            data_date = datetime.datetime.strptime(price_date, '%Y%m%d').date()
            # Get query handler, based on app config
            query_handler = current_app.config['feed_status_query_handler']
            # Get feeds' statuses
            feeds_with_statuses = query_handler.handle(data_date)  # query_handler.repo.get(data_date, feeds)
            # Format into dict (desired format for result)
            result_data = {
                fws.feed.name: {
                    'status': fws.status,
                    'asofdate': fws.status_ts.isoformat(),
                    'normal_eta': fws.feed.get_normal_eta(fws.data_date).isoformat(),
                    'security_type': fws.feed.security_type,
                } for fws in feeds_with_statuses
            }
            # Return standard format
            return self.formatter.success_get(result_data)
        except Exception as e:
            return self.formatter.exception(e)

@api.route('/api/transaction/<string:trade_date>')
class TransactionByDate(Resource):
    def get(self, trade_date):
        return  # TODO_WAVE3: implement

@api.route('/api/pricing/audit-reason')
class PricingAuditReason(Resource):
    formatter = DefaultRESTFormatter()

    def get(self):
        # Get query handler, based on app config
        query_handler = current_app.config['audit_reason_query_handler']
        # Get reasons
        reasons = query_handler.handle()
        # Return standard format
        return self.formatter.success_get(reasons)

@api.route('/api/pricing/attachment/<string:price_date>')
class PricingAttachmentByDate(Resource):
    formatter = DefaultRESTFormatter()
    
    def post(self, price_date):
        payload = api.payload
        try:
            command_handler = current_app.config['pricing_attachment_by_date_command_handler']
            row_cnt = command_handler.handle_post(price_date, payload)
            return self.formatter.success_post(row_cnt)
        except Exception as e:
            return self.formatter.exception(e)
    
    def get(self, price_date):
        try:
            # Get query handler, based on app config
            query_handler = current_app.config['pricing_attachment_by_date_query_handler']
            # Get feeds' statuses
            date_with_attachments = query_handler.handle(price_date)
            # Need to format into list (desired format for result):
            result_data = [{'full_path': f.full_path} for f in date_with_attachments.attachments]
            # Return standard format
            return self.formatter.success_get(result_data)
        except Exception as e:
            return self.formatter.exception(e)

@api.route('/api/pricing/held-security-price')
class HeldSecurityWithPrices(Resource):
    formatter = DefaultRESTFormatter()

    def post(self):
        # Note this is not really a standard "post" as it does not save data - but is created as such 
        # because we want to accept optional params in the payload rather than the URL, and 
        # some clients such as AngularJS cannot do so for a GET request.
        payload = api.payload
        try:
            # Get query handler, based on app config
            query_handler = current_app.config['held_security_price_query_handler']

            # Get counts
            result_data = query_handler.handle(payload)

            return self.formatter.success_get(result_data)
        except Exception as e:
            return self.formatter.exception(e)

@api.route('/api/pricing/price')
class PriceByIMEX(Resource):
    formatter = DefaultRESTFormatter()
    
    def post(self):
        payload = api.payload
        try:
            command_handler = current_app.config['price_by_imex_command_handler']
            row_cnt = command_handler.handle_post(payload)
            return self.formatter.success_post(row_cnt)
        except Exception as e:
            return self.formatter.exception(e)

@api.route('/api/pricing/manual-pricing-security')
class ManualPricingSecurity(Resource):
    formatter = DefaultRESTFormatter()

    def post(self):
        payload = api.payload
        try:
            command_handler = current_app.config['manual_pricing_security_command_handler']
            row_cnt = command_handler.handle_post(payload)
            return self.formatter.success_post(row_cnt)
        except Exception as e:
            return self.formatter.exception(e)
    
    def get(self):
        try:
            # Get query handler, based on app config
            query_handler = current_app.config['manual_pricing_security_query_handler']
            # Get feeds' statuses
            manual_pricing_securities = query_handler.handle()  # query_handler.repo.get(data_date, feeds)
            # manual_pricing_securities should be a list of Securities.
            # Need to format into list (desired format for result):
            result_data = [{'lw_id': sec.lw_id} for sec in manual_pricing_securities]
            # Return standard format
            return self.formatter.success_get(result_data)
        except Exception as e:
            return self.formatter.exception(e)

    def delete(self):
        payload = api.payload
        try:
            command_handler = current_app.config['manual_pricing_security_command_handler']
            row_cnt = command_handler.handle_delete(payload)
            return self.formatter.success_delete(row_cnt)
        except Exception as e:
            return self.formatter.exception(e)
    
@api.route('/api/pricing/audit-trail/<string:price_date>')
class PricingAuditTrail(Resource):
    def post(self, price_date):
        payload = api.payload
        return  # TODO: implement

    def get(self, price_date):
        return  # TODO: implement

@api.route('/api/pricing/audit-trail-v2/<string:price_date>')
class PricingAuditTrailv2(Resource):
    def post(self, price_date):
        payload = api.payload
        return  # TODO: implement

    def get(self, price_date):
        return  # TODO: implement

@api.route('/api/pricing/column-config/<string:user_id>')
class PricingColumnConfig(Resource):
    formatter = DefaultRESTFormatter()

    def post(self, user_id):
        payload = api.payload
        try:
            command_handler = current_app.config['column_config_command_handler']
            row_cnt = command_handler.handle_post(user_id, payload)
            return self.formatter.success_post(row_cnt)
        except Exception as e:
            return self.formatter.exception(e)

    def get(self, user_id):
        try:
            # Get query handler, based on app config
            query_handler = current_app.config['column_config_query_handler']
            # Get feeds' statuses
            user_with_column_config = query_handler.handle(user_id)  # query_handler.repo.get(data_date, feeds)
            # manual_pricing_securities should be a list of Securities.
            # Need to format into list (desired format for result):
            result_data = [{'user_id': user_id, 'column_name': cc.column_name, 'is_hidden': cc.is_hidden} for cc in user_with_column_config.column_configs]
            # Return standard format
            return self.formatter.success_get(result_data)
        except Exception as e:
            return self.formatter.exception(e)

    def delete(self, user_id):
        payload = api.payload
        try:
            command_handler = current_app.config['column_config_command_handler']
            row_cnt = command_handler.handle_delete(user_id, payload)
            return self.formatter.success_post(row_cnt)
        except Exception as e:
            return self.formatter.exception(e)
    
@api.route('/api/pricing/count-by-source')
class PriceCountBySource(Resource):
    formatter = DefaultRESTFormatter()

    def post(self):
        # Note this is not really a standard "post" as it does not save data - but is created as such 
        # because we want to accept optional params in the payload rather than the URL, and 
        # some clients such as AngularJS cannot do so for a GET request.
        payload = api.payload
        try:
            # Get query handler, based on app config
            query_handler = current_app.config['counts_by_source_query_handler']

            # Get counts
            result_data = query_handler.handle(payload)

            return self.formatter.success_get(result_data)
        except Exception as e:
            return self.formatter.exception(e)

