
# pypi
from marshmallow import Schema, fields, validates, ValidationError


class ManualPricingSecuritySchema(Schema):
    lw_id = fields.List(fields.String(required=True), required=True)

    @validates('lw_id')
    def validate_lw_id(self, value):
        if not isinstance(value, list):
            raise ValidationError("lw_id must be an array")




