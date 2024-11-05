import mysql.connector
import pandas as pd
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Database connection parameters from .env
db_config = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME')
}

# Query to select data from WordPress tables (e.g., posts, comments)
query = """
SELECT 
    p.ID AS post_id,
    p.post_title,
    p.post_content,
    p.post_date,
    u.display_name AS author_name,
    GROUP_CONCAT(t.name SEPARATOR ', ') AS tags
FROM 
    wp_posts p
LEFT JOIN 
    wp_users u ON p.post_author = u.ID
LEFT JOIN 
    wp_term_relationships tr ON p.ID = tr.object_id
LEFT JOIN 
    wp_term_taxonomy tt ON tr.term_taxonomy_id = tt.term_taxonomy_id
LEFT JOIN 
    wp_terms t ON tt.term_id = t.term_id
WHERE 
    p.post_type = 'post' AND p.post_status = 'publish'
GROUP BY 
    p.ID;
"""

try:
    # Connect to the database
    connection = mysql.connector.connect(**db_config)
    
    # Read data into a DataFrame
    df = pd.read_sql(query, connection)
    
    # Save DataFrame to a CSV file
    df.to_csv('wordpress_data.csv', index=False)
    print("Data exported to wordpress_data.csv successfully.")

except mysql.connector.Error as err:
    print(f"Error: {err}")

finally:
    if connection.is_connected():
        connection.close()
