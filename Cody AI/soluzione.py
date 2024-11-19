import mysql.connector
import logging
import os
from datetime import datetime
from decimal import Decimal
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BankReportGenerator:
    def __init__(self, host="localhost", user="root", password="root", database="bank"):
        self.db_config = {
            "host": host,
            "user": user,
            "password": password,
            "database": database
        }
        self.reports_dir = "reports"
        os.makedirs(self.reports_dir, exist_ok=True)

    def connect_db(self):
        return mysql.connector.connect(**self.db_config)

    def process_all_users(self):
        try:
            conn = self.connect_db()
            cursor = conn.cursor(dictionary=True)
            
            # Get all users
            cursor.execute("SELECT id, nome, primo_deposito, saldo FROM utenti")
            users = cursor.fetchall()
            
            for user in users:
                logger.info(f"Processing user {user['nome']} (ID: {user['id']})")
                self.process_single_user(conn, user)
                
        except Exception as e:
            logger.error(f"Error processing users: {str(e)}")
            raise
        finally:
            if 'conn' in locals():
                conn.close()

    def process_single_user(self, conn, user):
        cursor = conn.cursor(dictionary=True)
        try:
            # Start transaction
            conn.start_transaction()
            
            # Get user operations
            cursor.execute("""
                SELECT giorno, ammontare 
                FROM operazioni 
                WHERE utente_id = %s 
                ORDER BY giorno ASC
            """, (user['id'],))
            operations = cursor.fetchall()
            
            # Calculate new balance
            new_balance = user['primo_deposito'] + sum(op['ammontare'] for op in operations)
            
            # Generate report
            report_path = os.path.join(self.reports_dir, f"{user['id']}.txt")
            self.generate_report(user, operations, report_path)
            
            # Update user balance
            cursor.execute("""
                UPDATE utenti 
                SET saldo = %s 
                WHERE id = %s
            """, (new_balance, user['id']))
            
            # Commit transaction
            conn.commit()
            logger.info(f"Successfully processed user {user['id']}")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error processing user {user['id']}: {str(e)}")
            raise
            
    def generate_report(self, user, operations, report_path):
        try:
            with open(report_path, 'w', encoding='utf-8') as f:
                # Write user name
                f.write(f"{user['nome']}\n\n")
                
                # Write operations if any
                if operations:
                    for op in operations:
                        date_str = op['giorno'].strftime('%d/%m/%Y')
                        amount = op['ammontare']
                        formatted_amount = f"€{amount:>10,.2f}".replace(",", "'")
                        f.write(f"{date_str} ** {formatted_amount}\n")
                        
        except Exception as e:
            logger.error(f"Error generating report for user {user['id']}: {str(e)}")
            raise

def main():
    try:
        generator = BankReportGenerator()
        generator.process_all_users()
        logger.info("Processing completed successfully")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
