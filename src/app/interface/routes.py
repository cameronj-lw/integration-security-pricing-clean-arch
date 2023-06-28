
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
        return  # TODO: implement
    
    def get(self):
        return  # TODO: implement

    def delete(self):
        payload = api.payload
        return  # TODO: implement


@api.route('/api/pricing/feed-status')
class PricingFeedStatus(Resource):
    def get(self):
        return  # TODO: implement
            
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
            result_dict = {
                fws.feed.name: {
                    'status': fws.status,
                    'asofdate': fws.status_ts.isoformat(),
                    'normal_eta': fws.feed.get_normal_eta(fws.data_date).isoformat(),
                    'security_type': fws.feed.security_type,
                } for fws in feeds_with_statuses
            }
            # Return standard format
            return self.formatter.success_get(result_dict)
        except Exception as e:
            return self.formatter.exception(e)

@api.route('/api/transaction/<string:trade_date>')
class TransactionByDate(Resource):
    def get(self, trade_date):
        return  # TODO: implement

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
    def post(self, price_date):
        payload = api.payload
        return  # TODO: implement
    
    def get(self, price_date):
        return  # TODO: implement

    def delete(self, price_date):
        return  # TODO: implement

@api.route('/api/pricing/held-security-price')
class HeldSecurityWithPrices(Resource):
    def post(self):
        # Note this is not really a standard "post" as it does not save data - but is created as such 
        # because we want to accept optional params in the payload rather than the URL, and 
        # some clients such as AngularJS cannot do so for a GET request.
        payload = api.payload
        return  # TODO: implement

@api.route('/api/pricing/price/<string:price_date>')
class PriceByDate(Resource):
    def get(self, price_date):
        return  # TODO: implement

@api.route('/api/pricing/price')
class PriceByIMEX(Resource):
    
    def post(self):
        payload = api.payload
        return  # TODO: implement

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
            result_list = [{'lw_id': sec.lw_id} for sec in manual_pricing_securities]
            # Return standard format
            return self.formatter.success_get(result_list)
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
class PricingAuditTrail(Resource):
    def post(self, price_date):
        payload = api.payload
        return  # TODO: implement

    def get(self, price_date):
        return  # TODO: implement

@api.route('/api/pricing/column-config/<string:user_id>')
class PricingColumnConfig(Resource):
    def post(self, user_id):
        payload = api.payload
        return  # TODO: implement

    def get(self, user_id):
        return  # TODO: implement

    def delete(self, user_id):
        return  # TODO: implement
    
@api.route('/api/pricing/count-by-source')
class PriceCountBySource(Resource):  # TODO_WAVE4: implement
    def post(self):
        # Note this is not really a standard "post" as it does not save data - but is created as such 
        # because we want to accept optional params in the payload rather than the URL, and 
        # some clients such as AngularJS cannot do so for a GET request.
        payload = api.payload
        return  # TODO: implement

