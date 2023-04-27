import sqlite3


def create_and_persist_transformations(db_file):
    conn = sqlite3.connect(db_file)

    # Combine the two carrier files into one table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS combined_carrier AS
        SELECT * FROM DE1_0_2008_to_2010_Carrier_Claims_Sample_1A
        UNION ALL
        SELECT * FROM DE1_0_2008_to_2010_Carrier_Claims_Sample_1B
    """)

    # Create a table with carrier_claim_year and distinct_ms_member_count columns as the main output of this exercise.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ms_member_count_by_year AS
        SELECT
            SUBSTR(CAST(CLM_FROM_DT AS TEXT), 1, 4) AS carrier_claim_year,
            COUNT(DISTINCT DESYNPUF_ID) as distinct_ms_member_count
        FROM
            combined_carrier
        WHERE
            ICD9_DGNS_CD_1 = '340'
        GROUP BY
        carrier_claim_year
    """)

    conn.commit()
    conn.close()
