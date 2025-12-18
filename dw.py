import os
import sys
import duckdb  # https://duckdb.org
import pygrametl  # https://pygrametl.org
from pygrametl.tables import CachedDimension, FactTable


duckdb_filename = 'dw.duckdb'


class DW:
    def __init__(self, create=False):
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

                self.conn_duckdb.execute('''
                    CREATE TABLE days (
                        day_id VARCHAR PRIMARY KEY,
                        day INTEGER,
                        month_id VARCHAR
                    );
                    CREATE TABLE months (
                        month_id VARCHAR PRIMARY KEY,
                        month INTEGER,
                        year INTEGER
                    );
                    CREATE TABLE aircrafts (
                        registration VARCHAR PRIMARY KEY,
                        model VARCHAR,
                        manufacturer VARCHAR
                    );
                    CREATE TABLE reporteurs (
                        reporteur_uid VARCHAR PRIMARY KEY,
                        airport VARCHAR,
                        role VARCHAR
                    );
                    CREATE TABLE daily_usage (
                        registration VARCHAR,
                        day_id VARCHAR,
                        fh DECIMAL(10, 2),
                        tos INTEGER,
                        sto INTEGER,
                        FOREIGN KEY(registration) REFERENCES aircrafts(registration),
                        FOREIGN KEY(day_id) REFERENCES days(day_id)
                    );
                    CREATE TABLE monthly_usage (
                        registration VARCHAR,
                        month_id VARCHAR,
                        dy INTEGER,
                        cn INTEGER,
                        dh DECIMAL(10, 2),
                        ados DECIMAL(10, 2),
                        adoss DECIMAL(10, 2),
                        adosu DECIMAL(10, 2),
                        adis DECIMAL(10, 2),
                        FOREIGN KEY(registration) REFERENCES aircrafts(registration),
                        FOREIGN KEY(month_id) REFERENCES months(month_id)
                    );
                    CREATE TABLE reportage_usage (
                        registration VARCHAR,
                        month_id VARCHAR,
                        reporteur_uid VARCHAR,
                        reps INTEGER,
                        mareps INTEGER,
                        pireps INTEGER,
                        FOREIGN KEY(registration) REFERENCES aircrafts(registration),
                        FOREIGN KEY(month_id) REFERENCES months(month_id),
                        FOREIGN KEY(reporteur_uid) REFERENCES reporteurs(reporteur_uid)
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
            key='day_id',
            attributes=['day', 'month_id'],
        )

        months_dimension = CachedDimension(
            name='months',
            key='month_id',
            attributes=['month', 'year']
        )

        aircrafts_dimension = CachedDimension(
            name='aircrafts',
            key='registration',
            attributes=['model', 'manufacturer']
        )

        reporteurs_dimension = CachedDimension(
            name='reporteurs',
            key='reporteur_uid',
            attributes=['airport', 'role']
        )

        daily_usage_fact_table = FactTable(
            name='daily_usage',
            keyrefs=['registration', 'day_id'],
            measures=['fh', 'tos', 'sto']
        )

        monthly_usage_fact_table = FactTable(
            name='monthly_usage',
            keyrefs=['registration', 'month_id'], 
            measures=['dy', 'cn', 'dh', 'ados', 'adoss', 'adosu', 'adis']
        )

        reportage_usage_fact_table = FactTable(
            name='reportage_usage',
            keyrefs=['registration', 'month_id', 'reporteur_uid'],
            measures=['reps', 'mareps', 'pireps']
        )

        # Mapping from table name to table object
        self.tables_dict = {
            'days': days_dimension,
            'months': months_dimension,
            'aircrafts': aircrafts_dimension,
            'reporteurs': reporteurs_dimension,
            'daily_usage': daily_usage_fact_table,
            'monthly_usage': monthly_usage_fact_table,
            'reportage_usage': reportage_usage_fact_table
        }
    
    def get_table(self, name: str) -> CachedDimension|FactTable:
        return self.tables_dict.get(name)
    
    def restart(self):
        self.conn_duckdb.execute('''
                    DELETE FROM daily_usage;
                    DELETE FROM monthly_usage;
                    DELETE FROM reportage_usage;
                    DELETE FROM aircrafts;
                    DELETE FROM reporteurs;
                    DELETE FROM days;
                    DELETE FROM months;
                                 ''')



    # TODO: Rewrite the queries exemplified in "extract.py"
    def query_utilization(self):

        result = self.conn_duckdb.execute("""
                                          
            WITH year_daily_agg AS (
            SELECT
                a.manufacturer,
                m.year,
                SUM(du.fh) AS fh,
                SUM(du.tos) AS tos,
                SUM(du.sto) AS sto,
                COUNT(DISTINCT( a.registration )) AS n_aircrafts
 
            FROM daily_usage du, days d, months m, aircrafts a        
            WHERE du.day_id = d.day_id AND d.month_id = m.month_id AND a.registration = du.registration
            GROUP BY a.manufacturer, m.year
            ),
        
            year_monthly_agg AS (
            SELECT
                a.manufacturer,
                m.year,
                SUM(mu.ados) AS ados,
                SUM(mu.adoss) AS adoss,
                SUM(mu.adosu) AS adosu,
                SUM(mu.adis) AS adis,
                SUM(mu.dh) AS dh,
                SUM(mu.dy) AS dy,
                SUM(mu.cn) AS cn,
                                        
            FROM monthly_usage mu, months m, aircrafts a        
            WHERE mu.month_id = m.month_id AND a.registration = mu.registration
            GROUP BY a.manufacturer, m.year
            )

            SELECT  yda.manufacturer, yda.year,
                    ROUND(yda.fh/n_aircrafts, 2), 
                    ROUND(yda.tos/n_aircrafts, 2), 

                    ROUND(yma.adoss/n_aircrafts, 2),
                    ROUND(yma.adosu/n_aircrafts, 2),
                    ROUND(yma.ados/n_aircrafts, 2),
                    ROUND(yma.adis/n_aircrafts, 2),
                                            
                    ROUND(yda.fh/(24*yma.adis), 2)                      AS du,
                    ROUND(yda.tos/yma.adis, 2)                          AS dc,
                    
                    ROUND(100*yma.dy/(yda.sto), 2)                      AS dyr, 
                    ROUND(100*yma.cn/yda.tos, 2)                        AS cnr, 
                    ROUND(100*(1-(yma.dy+yma.cn)/yda.sto), 2)           AS tdr, 
                    ROUND(100*60*yma.dh/yma.dy, 2)                      AS add

            FROM year_monthly_agg yma, year_daily_agg as yda
            WHERE yma.year = yda.year AND yma.manufacturer = yda.manufacturer
            ORDER BY yma.manufacturer, yma.year;
                                          
                        
            """).fetchall()  
        return result




    def query_reporting(self):
        result = self.conn_duckdb.execute("""
            
            WITH year_daily_agg AS (
            SELECT
                a.manufacturer,
                m.year,
                SUM(du.fh) AS fh,
                SUM(du.tos) AS tos
            FROM daily_usage du, days d, months m, aircrafts a        
            WHERE du.day_id = d.day_id AND d.month_id = m.month_id AND a.registration = du.registration
            GROUP BY a.manufacturer, m.year
            ),
        
            year_reportage_agg AS (
            SELECT
                a.manufacturer,
                m.year,
                SUM(ru.reps) as reps                
            FROM reportage_usage ru, months m, aircrafts a        
            WHERE ru.month_id = m.month_id AND a.registration = ru.registration
            GROUP BY a.manufacturer, m.year
            )
                                    
            SELECT yra.manufacturer, yra.year, 
                    1000*ROUND(yra.reps/yda.fh, 3)              AS rrh, 
                    100*ROUND(yra.reps/yda.tos, 2)              AS rrc
                                          
            FROM year_daily_agg yda, year_reportage_agg yra
            WHERE yda.manufacturer = yra.manufacturer AND yda.year = yra.year
            ORDER BY yra.manufacturer, yra.year;
            
            """).fetchall()
        return result




    def query_reporting_per_role(self):
        result = self.conn_duckdb.execute("""
            
            WITH year_daily_agg AS (
            SELECT
                a.manufacturer,
                m.year,
                SUM(du.fh) AS fh,
                SUM(du.tos) AS tos
            FROM daily_usage du, days d, months m, aircrafts a        
            WHERE du.day_id = d.day_id AND d.month_id = m.month_id AND a.registration = du.registration
            GROUP BY a.manufacturer, m.year
            ),
        
            year_reportage_agg AS (
            SELECT
                a.manufacturer,
                m.year,
                SUM(ru.mareps) as mareps,     
                SUM(ru.mareps) as pireps               
            FROM reportage_usage ru, months m, aircrafts a        
            WHERE ru.month_id = m.month_id AND a.registration = ru.registration
            GROUP BY a.manufacturer, m.year
            )
                                    
            SELECT 
                yra.manufacturer,
                yra.year,
                'MAREP' AS role,
                1000 * ROUND(yra.mareps / yda.fh, 3) AS rrh,
                100  * ROUND(yra.mareps / yda.tos, 2) AS rrc
            FROM year_daily_agg yda
            JOIN year_reportage_agg yra 
                ON yda.manufacturer = yra.manufacturer 
                AND yda.year = yra.year

            UNION ALL

            SELECT 
                yra.manufacturer,
                yra.year,
                'PIREP' AS role,
                1000 * ROUND(yra.pireps / yda.fh, 3) AS rrh,
                100  * ROUND(yra.pireps / yda.tos, 2) AS rrc
            FROM year_daily_agg yda
            JOIN year_reportage_agg yra 
                ON yda.manufacturer = yra.manufacturer 
                AND yda.year = yra.year

            ORDER BY manufacturer, year, role;
            
            """).fetchall()
        return result


    def close(self):
        self.conn_pygrametl.commit()
        self.conn_pygrametl.close()
    
    def __enter__(self):
        return self  # Necesario para 'with'
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()  # Esto hace que 'with' funcione
