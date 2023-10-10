
# pypi
from marshmallow import Schema, fields, validate, validates, ValidationError


class LWIDSchema(Schema):
    lw_id = fields.List(fields.String(required=True), required=True)

    @validates('lw_id')
    def validate_lw_id(self, value):
        if not isinstance(value, list):
            raise ValidationError("lw_id must be an array")


class ColumnConfigSchema(Schema):
    column_name = fields.Str(required=True)
    is_hidden = fields.Bool(required=True)


class ColumnConfigPayloadSchema(Schema):
    columns = fields.List(fields.Nested(ColumnConfigSchema), required=True)


class FileSchema(Schema):
    name = fields.Str(required=True)
    binary_content = fields.Str(required=True)  # TODO: confirm if this will be str or other?


class FilePayloadSchema(Schema):
    files = fields.List(fields.Nested(FileSchema), required=True)


class PriceSchema(Schema):
    apx_sec_type = fields.String(required=True)
    apx_symbol = fields.String(required=True)
    source = fields.String(required=True)
    from_date = fields.Date(required=True)
    price = fields.Float()
    yield_ = fields.Float(data_key='yield')  # since yield is a python keyword
    duration = fields.Float()

    # Ensure at least one of price/yield/dur has a value
    # TODO: is there a less convoluted way to do this?
    
    @validates('price')
    def validate_price(self, value):
        if value is None and self.yield_ is None and self.duration is None:
            raise ValueError("At least one of 'price', 'yield', or 'duration' is required.")

    @validates('yield_')
    def validate_yield(self, value):
        if value is None and self.price is None and self.duration is None:
            raise ValueError("At least one of 'price', 'yield', or 'duration' is required.")

    @validates('duration')
    def validate_duration(self, value):
        if value is None and self.price is None and self.yield_ is None:
            raise ValueError("At least one of 'price', 'yield', or 'duration' is required.")


class PriceByIMEXPayloadSchema(Schema):
    prices = fields.List(fields.Nested(PriceSchema), required=True)

class IMEXPayloadSchema(Schema):
    mode = fields.String(required=True)
    full_path = fields.String(required=True)

class PriceByIMEXSchema(Schema):
    payload = fields.Nested(PriceByIMEXPayloadSchema)


