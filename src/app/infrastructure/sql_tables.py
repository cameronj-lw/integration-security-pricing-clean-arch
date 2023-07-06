


import sqlalchemy
from sqlalchemy import sql

from app.infrastructure.util.table import BaseTable, ScenarioTable


"""
COREDB
"""

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


