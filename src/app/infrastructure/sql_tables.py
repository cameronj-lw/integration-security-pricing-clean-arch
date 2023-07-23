

import logging
import sqlalchemy
from sqlalchemy import sql, bindparam

from app.infrastructure.util.table import BaseTable, ScenarioTable


"""
APXDB
"""

class APXDBvPriceTable(BaseTable):
	config_section = 'apxdb'
	table_name = 'vPrice'
	schema = 'APX'

	def read(self, security_id=None, price_date=None, price_type_id=None):
		"""
		Read all entries, optionally by SecurityID/PriceDate/PriceTypeID

		:return: DataFrame
		"""
		stmt = None
		if sqlalchemy.__version__ >= '2':
			stmt = sql.select(self.table_def)
		else:
			stmt = sql.select([self.table_def])
		if security_id is not None:
			stmt = stmt.where(self.c.SecurityID == security_id)
		if price_date is not None:
			stmt = stmt.where(self.c.PriceDate == price_date)
		if price_type_id is not None:
			stmt = stmt.where(self.c.PriceTypeID == price_type_id)
		return self.execute_read(stmt)


"""
COREDB
"""

class CoreDBPriceAuditEntryTable(BaseTable):
	config_section = 'coredb'
	schema = 'pricing'
	table_name = 'audit_trail'

	def read(self, data_date=None, security=None):
		"""
		Read all entries, optionally for date/security

		:param data_date (datetime.date): price date for the audit trail to get.
		:param security (Security): security to get audit trail for.
		:return: DataFrame
		"""		
		stmt = None
		if sqlalchemy.__version__ >= '2':
			stmt = sql.select(self.table_def)
		else:
			stmt = sql.select([self.table_def])
		if security is not None:
			stmt = stmt.where(self.c.lw_id == security.lw_id)
		if data_date is not None:
			stmt = stmt.where(self.c.data_date == data_date)
		return self.execute_read(stmt)

class CoreDBManualPricingSecurityTable(BaseTable):
	config_section = 'coredb'
	schema = 'pricing'
	table_name = 'manual_pricing_security'

	def read(self, exclude_deleted=True):
		"""
		Read all entries, excluding those with "is_deleted" by default

		:param exclude_deleted: Whether to exclude those with "is_deleted"
		:return: DataFrame
		"""		
		stmt = None
		if sqlalchemy.__version__ >= '2':
			stmt = sql.select(self.table_def)
		else:
			stmt = sql.select([self.table_def])
		if exclude_deleted:
			stmt = stmt.where(self.c.is_deleted == False)
		return self.execute_read(stmt)


class CoreDBColumnConfigTable(BaseTable):
	config_section = 'coredb'
	schema = 'pricing'
	table_name = 'column_config'

	def read(self, user_id=None, exclude_deleted=True):
		"""
		Read all entries, optionally for a user_id, excluding those with "is_deleted" by default

		:param user_id: user_id to query for
		:param exclude_deleted: Whether to exclude those with "is_deleted"
		:return: DataFrame
		"""		
		stmt = None
		if sqlalchemy.__version__ >= '2':
			stmt = sql.select(self.table_def)
		else:
			stmt = sql.select([self.table_def])
		if user_id is not None:
			stmt = stmt.where(self.c.user_id == user_id)
		if exclude_deleted:
			stmt = stmt.where(self.c.is_deleted == False)
		return self.execute_read(stmt)


class CoreDBvwPriceView(BaseTable):
	config_section = 'coredb'
	table_name = 'vw_price'

	def read(self, data_date=None, source_name=None, lw_id=None):
		"""
		Read all entries, optionally filtering by date/source/lw_id(s)

		:return: DataFrame
		"""
		stmt = None
		if sqlalchemy.__version__ >= '2':
			stmt = sql.select(self.table_def)
		else:
			stmt = sql.select([self.table_def])
		if data_date is not None:
			stmt = stmt.where(self.c.data_date == data_date)
		if source_name is not None:
			stmt = stmt.where(self.c.source == source_name)
		if lw_id is not None:
			if isinstance(lw_id, str):
				stmt = stmt.where(self.c.lw_id == lw_id)
			elif isinstance(lw_id, list):
				stmt = stmt.where(self.c.lw_id.in_(lw_id))
		return self.execute_read(stmt)


class CoreDBvwSecurityView(BaseTable):
	config_section = 'coredb'
	table_name = 'vw-security'

	def read(self, lw_id=None):
		"""
		Read all entries, optionally for specific lw_id(s)

		:return: DataFrame
		"""
		stmt = None
		if sqlalchemy.__version__ >= '2':
			stmt = sql.select(self.table_def)
		else:
			stmt = sql.select([self.table_def])
		if lw_id is not None:
			if isinstance(lw_id, str):
				stmt = stmt.where(self.c.lw_id == lw_id)
			elif isinstance(lw_id, list):
				stmt = stmt.where(self.c.lw_id.in_(lw_id))
		return self.execute_read(stmt)


class CoreDBvwPriceBatchView(BaseTable):
	config_section = 'coredb'
	schema = 'pricing'
	table_name = 'vw-price-batch'

	def read(self, data_date=None, source_name=None):
		"""
		Read all entries

		:return: DataFrame
		"""
		stmt = None
		if sqlalchemy.__version__ >= '2':
			stmt = sql.select(self.table_def)
		else:
			stmt = sql.select([self.table_def])
		if data_date is not None:
			stmt = stmt.where(self.c.data_date == data_date)
		if source_name is not None:
			stmt = stmt.where(self.c.source == source_name)
		return self.execute_read(stmt)


"""
LWDB
"""

class LWDBCalendarTable(ScenarioTable):
	config_section = 'lwdb'
	table_name = 'calendar'

	def read_for_date(self, data_date):
		"""
		Read all entries for a specific date and with the latest scenario

		:param data_date: The data date
		:return: DataFrame
		"""
		if sqlalchemy.__version__ >= '2':
			stmt = sql.select(self.table_def)
		else:
			stmt = sql.select([self.table_def])
		stmt = (
			stmt
			.where(self.c.scenario == self.base_scenario)
			.where(self.c.data_dt == data_date)
		)
		data = self.execute_read(stmt)
		return data


class LWDBAPXAppraisalTable(ScenarioTable):
	config_section = 'lwdb'
	table_name = 'apx_appraisal'

	def read_for_date(self, data_date):
		"""
		Read all entries for a specific date and with the latest scenario

		:param data_date: The data date
		:return: DataFrame
		"""
		if sqlalchemy.__version__ >= '2':
			stmt = sql.select(self.table_def)
		else:
			stmt = sql.select([self.table_def])
		stmt = (
			stmt
			.where(self.c.scenario == self.base_scenario)
			.where(self.c.data_dt == data_date)
		)
		data = self.execute_read(stmt)
		return data


class LWDBPricingTable(ScenarioTable):
	config_section = 'lwdb'
	table_name = 'pricing'

	def read_for_date(self, data_date):
		"""
		Read all entries for a specific date and with the latest scenario

		:param data_date: The data date
		:return: DataFrame
		"""
		if sqlalchemy.__version__ >= '2':
			stmt = sql.select(self.table_def)
		else:
			stmt = sql.select([self.table_def])
		stmt = (
			stmt
			.where(self.c.scenario == self.base_scenario)
			.where(self.c.data_dt == data_date)
		)
		data = self.execute_read(stmt)
		return data


"""
MGMTDB
"""

class MGMTDBMonitorTable(ScenarioTable):
	config_section = 'mgmtdb'
	table_name = 'monitor'

	def read(self, scenario=None, data_date=None, run_group=None, run_name=None, run_type=None):
		"""
		Read all entries, optionally with criteria

		:return: DataFrame
		"""
		stmt = sql.select(self.table_def)
		if scenario is not None:
			stmt = stmt.where(self.c.scenario == scenario)
		if data_date is not None:
			stmt = stmt.where(self.c.data_dt == data_date)
		if run_group is not None:
			stmt = stmt.where(self.c.run_group == run_group)
		if run_name is not None:
			stmt = stmt.where(self.c.run_name == run_name)
		if run_type is not None:
			stmt = stmt.where(self.c.run_type == run_type)
		return self.execute_read(stmt)

	def read_for_date(self, data_date):
		"""
		Read all entries for a specific date

		:param data_date: The data date
		:returns: DataFrame
		"""
		stmt = (
			sql.select(self.table_def)
			.where(self.c.data_dt == data_date)
		)
		data = self.execute_read(stmt)
		return data


