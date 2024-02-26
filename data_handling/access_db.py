import psycopg2

try:
    # Connect to the database
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        dbname="ai_cc",
        user="postgres",
        password="ai_cc_24"
    )

    # Create a cursor
    cursor = conn.cursor()

    # Execute the query
    cursor.execute("SELECT * FROM scopus_data")

    # Fetch all the records
    records = cursor.fetchall()

    # Print the number of records
    print("Number of records:", cursor.rowcount)

except psycopg2.Error as err:
    print("Error occurred while accessing the database:", err)

finally:
    # Close the cursor and connection
    if cursor:
        cursor.close()
    if conn:
        conn.close()