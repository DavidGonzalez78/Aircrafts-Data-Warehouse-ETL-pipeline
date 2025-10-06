import os
import sys
import duckdb  # https://duckdb.org
import pygrametl  # https://pygrametl.org
from pygrametl.tables import CachedDimension, SnowflakedDimension, FactTable


duckdb_filename = 'dw.duckdb'


class DW:
    def _init_(self, create=False):
        if create and os.path.exists(duckdb_filename):
            os.remove(duckdb_filename)
        try:
            self.conn_duckdb = duckdb.connect(duckdb_filename)
            print("Connection to the DW created successfully")
        except duckdb.Error as e:
            print(f"Unable to connect to DuckDB database '{duckdb_filename}':", e)
            sys.exit(1)

        if create:
            try:
                # TODO: Create the tables in the DW (DONE)
                self.conn_duckdb.execute('''
                    CREATE TABLE days (
                        day INTEGER PRIMARY KEY,
                        month INTEGER
                    );
                    CREATE TABLE months (
                        month INTEGER PRIMARY KEY,
                        year INTEGER
                    );
                    CREATE TABLE aircrafts (
                        registration VARCHAR PRIMARY KEY,
                        model VARCHAR
                    );
                    CREATE TABLE reporters (
                        reporter_uid VARCHAR PRIMARY KEY,
                        airport VARCHAR
                    );
                    CREATE TABLE daily_kpis (
                        day INTEGER,
                        registration VARCHAR,
                        fh INTEGER,
                        tos INTEGER,
                        FOREIGN KEY(day) REFERENCES days(day),
                        FOREIGN KEY(registration) REFERENCES aircrafts(registration)
                    );
                    CREATE TABLE monthly_kpis (
                        month INTEGER,
                        registration VARCHAR,
                        reporter_uid VARCHAR,
                        adis INTEGER,
                        ados INTEGER,
                        dyr INTEGER,
                        cnr INTEGER,
                        tdr INTEGER,
                        add INTEGER,
                        rrh INTEGER,
                        rrc INTEGER,
                        prrh INTEGER,
                        prrc INTEGER,
                        mrrh INTEGER,
                        mrrc INTEGER,
                        FOREIGN KEY(month) REFERENCES months(month),
                        FOREIGN KEY(registration) REFERENCES aircrafts(registration),
                        FOREIGN KEY(reporter_uid) REFERENCES reporters(reporter_uid)
                    );
                    ''')
                print("Tables created successfully")
            except duckdb.Error as e:
                print("Error creating the DW tables:", e)
                sys.exit(2)

        # Link DuckDB and pygrametl
        self.conn_pygrametl = pygrametl.ConnectionWrapper(self.conn_duckdb)

        # ======================================================================================================= Dimension and fact table objects
        # TODO: Declare the dimensions and facts for pygrametl (DONE)

        days_dimension = CachedDimension(
            name='days',
            key='day',
            attributes=['month']
        )

        months_dimension = CachedDimension(
            name='months',
            key='month',
            attributes=['year']
        )

        dates_dimension = SnowflakedDimension(references=[
            (days_dimension, months_dimension)
        ])

        aircrafts_dimension = CachedDimension(
            name='aircrafts',
            key='registration',
            attributes=['model', 'manufacturer']
        )

        reporters_dimension = CachedDimension(
            name='reporters',
            key='reporter_uid',
            attributes=['airport']
        )

        daily_kpis_fact_table = FactTable(
            name='daily_kpis',
            keyrefs=['day', 'registration']
        )

        monthly_kpis_fact_table = FactTable(
            name='monthly_kpis',
            keyrefs=['month', 'registration', 'register_uid']
        )

    # TODO: Rewrite the queries exemplified in "extract.py"
    def query_utilization(self):
        result = self.conn_duckdb.execute("""
            SELECT a.manufacturer, m.year, dk.fh, dk.tos, mk.adis, mk.ados, mk.dyr, mk.cnr, mk.tdr, mk.add
            FROM daily_kpis dk, monthly_kpis mk, days d, months m, aircrafts a
            WHERE dk.registration = mk.registration = a.registration AND dk.day = d.day AND mk.month = m.month = d.month
            GROUP BY a.manufacturer, m.year
            ORDER BY a.manufacturer, m.year;                           
            """).fetchall() # type: ignore
        return result

    def query_reporting(self):
        result = self.conn_duckdb.execute("""
            SELECT ...
            """).fetchall()
        return result

    def query_reporting_per_role(self):
        result = self.conn_duckdb.execute("""
            SELECT ...
            """).fetchall()
        return result

    def close(self):
        self.conn_pygrametl.commit()
        self.conn_pygrametl.close()