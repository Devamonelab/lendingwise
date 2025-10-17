import mysql.connector
from mysql.connector import Error
from datetime import datetime
import uuid


def connect_to_database():
    """Connect to the MySQL database and return the connection object."""
    try:
        connection = mysql.connector.connect(
            host='3.129.145.187',
            port=3306,
            user='aiagentdb',
            password='Agents@1252',  # <-- replace with your password
            database='stage_newskinny'      # <-- replace with your schema name
        )
        if connection.is_connected():
            print("✅ Connected to MySQL Database")
            return connection
    except Error as e:
        print(f"❌ Error while connecting to MySQL: {e}")
        return None


def create_tblaiagents_table(connection):
    """Create table tblaiagents if it does not already exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS tblaiagents (
        id VARCHAR(36) PRIMARY KEY,
        FPCID VARCHAR(255),
        LMRId VARCHAR(255),
        document_name VARCHAR(255),
        agent_name VARCHAR(255),
        tool VARCHAR(255),
        file_s3_location TEXT DEFAULT NULL,
        date DATE,
        document_status VARCHAR(255) DEFAULT NULL,
        uploadedat DATETIME DEFAULT NULL,
        metadata_s3_path TEXT DEFAULT NULL,
        verified_result_s3_path TEXT DEFAULT NULL,
        cross_validation BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        Is_varified BOOLEAN DEFAULT FALSE        
    ) ENGINE=InnoDB;
    """
    try:
        cursor = connection.cursor()
        cursor.execute(create_table_query)
        connection.commit()
        print("✅ Table 'tblaiagents' created successfully (if not exists).")
    except Error as e:
        print(f"❌ Failed to create table: {e}")
    finally:
        cursor.close()


def insert_data_into_tblaiagents(connection, FPCID, LMRId, document_name, agent_name, tool, date, created_at=None):
    """Insert a new record into tblaiagents with id as UUID and cross_validation defaulting to FALSE."""
    insert_query = """
    INSERT INTO tblaiagents (
        id, FPCID, LMRId, document_name, agent_name, tool, date, cross_validation, created_at, Is_varified
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    record_id = str(uuid.uuid4())
    if created_at is None:
        created_at = datetime.now()

    try:
        cursor = connection.cursor()
        cursor.execute(insert_query, (
            record_id,
            FPCID,
            LMRId,
            document_name,
            agent_name,
            tool,
            date,
            False,  # default value for cross_validation
            created_at,
            False  # default value for Is_varified
        ))
        connection.commit()
        print(f"✅ Data inserted successfully (UUID: {record_id})")
    except Error as e:
        print(f"❌ Failed to insert data: {e}")
    finally:
        cursor.close()


def update_cross_validation_status(connection, record_id, status=True):
    """Update the cross_validation status (default: set to True)."""
    update_query = """
    UPDATE tblaiagents
    SET cross_validation = %s
    WHERE id = %s;
    """
    try:
        cursor = connection.cursor()
        cursor.execute(update_query, (status, record_id))
        connection.commit()
        print(f"✅ Cross_validation updated to {status} for record ID: {record_id}")
    except Error as e:
        print(f"❌ Failed to update cross_validation: {e}")
    finally:
        cursor.close()


def main():
    connection = connect_to_database()
    if connection:
        create_tblaiagents_table(connection)

        # Example insert (you can modify these values as needed)
        insert_data_into_tblaiagents(
            connection,
            FPCID="3363",
            LMRId="45",
            document_name="Driving license",
            agent_name="Identity Verification Agent",
            tool="ocr+llm",
            date="2025-10-17"
        )
        connection.close()
        print("🔒 Connection closed.")


if __name__ == "__main__":
    main()
