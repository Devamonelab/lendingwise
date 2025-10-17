import mysql.connector
from mysql.connector import Error
from datetime import datetime


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


def create_tblfile_table(connection):
    """Create table tblfile if it does not already exist, with nullable INT defaults."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS tblfile (
        LMRId VARCHAR(255),
        brokerNumber INT NULL DEFAULT 0,
        secondaryBrokerNumber VARCHAR(255),
        FBRID INT NULL DEFAULT 0,
        FPCID VARCHAR(255),
        borrowerName VARCHAR(255),
        occupancy VARCHAR(255),
        borrowerLoanRate VARCHAR(255),
        loanType VARCHAR(255),
        adjustableDate VARCHAR(255),
        mortgageLates VARCHAR(255),
        borrowerLName VARCHAR(255),
        activeStatus INT NULL DEFAULT 0,
        isCoBorrower INT NULL DEFAULT 0,
        noOfPeopleInProperty INT NULL DEFAULT 0,
        areTaxesInsuranceEscrowed INT NULL DEFAULT 0,
        LMREmailSent INT NULL DEFAULT 0,
        clientId INT NULL DEFAULT 0,
        packageViewed INT NULL DEFAULT 0,
        referralCode1_del INT NULL DEFAULT 0,
        LMRAffiliateCode1_del INT NULL DEFAULT 0,
        executiveId1_del INT NULL DEFAULT 0,
        leadID INT NULL DEFAULT 0,
        mortgageOwner1 INT NULL DEFAULT 0,
        mortgageOwner2 INT NULL DEFAULT 0,
        FAFeePaid INT NULL DEFAULT 0,
        areInsuranceEscrowed INT NULL DEFAULT 0,
        REBrokerId INT NULL DEFAULT 0,
        encFieldUpdate INT NULL DEFAULT 0,
        fileCopied INT NULL DEFAULT 0,
        oldFPCID INT NULL DEFAULT 0,
        borrowerMName VARCHAR(255),
        coBorrowerFName VARCHAR(255),
        coBorrowerLName VARCHAR(255),
        borrowerDOB DATE,
        coBorrowerDOB DATE,
        coBorDriverLicenseNumber VARCHAR(255),
        coBorDriverLicenseState VARCHAR(255),
        driverLicenseNumber VARCHAR(255),
        driverLicenseState VARCHAR(255),
        borrowerPOB VARCHAR(255),
        userType VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB;
    """
    try:
        cursor = connection.cursor()
        cursor.execute(create_table_query)
        connection.commit()
        print("✅ Table 'tblfile' created successfully (INT columns now nullable with default 0).")
    except Error as e:
        print(f"❌ Failed to create table: {e}")
    finally:
        cursor.close()



def insert_into_tblfile(connection, FPCID, LMRId, brokerNumber, clientId,
                        borrowerLName, borrowerName, borrowerMName,
                        coBorrowerFName, coBorrowerLName, borrowerDOB, coBorrowerDOB,
                        coBorDriverLicenseNumber, coBorDriverLicenseState,
                        driverLicenseNumber, driverLicenseState,
                        borrowerPOB, userType):
    """Insert a new record including brokerNumber and clientId."""
    insert_query = """
    INSERT INTO tblfile (
        FPCID, LMRId, brokerNumber, clientId, borrowerLName, borrowerName, borrowerMName,
        coBorrowerFName, coBorrowerLName, borrowerDOB, coBorrowerDOB,
        coBorDriverLicenseNumber, coBorDriverLicenseState,
        driverLicenseNumber, driverLicenseState, borrowerPOB, userType
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    try:
        cursor = connection.cursor()
        cursor.execute(insert_query, (
            FPCID, LMRId, brokerNumber, clientId, borrowerLName, borrowerName, borrowerMName,
            coBorrowerFName, coBorrowerLName, borrowerDOB, coBorrowerDOB,
            coBorDriverLicenseNumber, coBorDriverLicenseState,
            driverLicenseNumber, driverLicenseState, borrowerPOB, userType
        ))
        connection.commit()
        print("✅ Record inserted successfully with brokerNumber and clientId included!")
    except Error as e:
        print(f"❌ Failed to insert data: {e}")
    finally:
        cursor.close()



def main():
    connection = connect_to_database()
    if connection:
        create_tblfile_table(connection)

        # Example insert
        insert_into_tblfile(
            connection,
            FPCID="3363",
            LMRId="45",
            brokerNumber=37850,
            borrowerLName="MKINLEY",
            borrowerName="DENNIS",
            borrowerMName="DEN",
            coBorrowerFName="",
            coBorrowerLName="",
            borrowerDOB="1980-07-25",
            coBorrowerDOB="1984-03-21",
            clientId=5863051,
            coBorDriverLicenseNumber="",
            coBorDriverLicenseState="",
            driverLicenseNumber="90862014",
            driverLicenseState="GA",
            borrowerPOB="",
            userType="primary"
        )

        connection.close()
        print("🔒 Connection closed.")


if __name__ == "__main__":
    main()
