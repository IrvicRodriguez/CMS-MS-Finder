import sqlite3

# Function to create and persist transformations in tables in the DB.
def create_and_persist_transformations(db_file):
    """
    Creates and persists transformations on the loaded data.

    Args:
        db_file (str): The path to the SQLite database file.

    Returns:
        None. main.py exists after this. I should add an output of finished to let other developers know.
    """
    conn = sqlite3.connect(db_file)

    # Combine the two carrier files into one table called combined_carrier
    conn.execute("""
        CREATE TABLE IF NOT EXISTS combined_carrier AS
        SELECT * FROM DE1_0_2008_to_2010_Carrier_Claims_Sample_1A
        UNION ALL
        SELECT * FROM DE1_0_2008_to_2010_Carrier_Claims_Sample_1B
    """)

    # Create a table with carrier_claim_year and distinct_ms_member_count columns as the main output.
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

    # Commit the changes and close the connection
    conn.commit()
    conn.close()
