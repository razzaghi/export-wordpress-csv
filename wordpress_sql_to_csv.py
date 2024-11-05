import mysql.connector
import pandas as pd
from dotenv import load_dotenv
import os
import html

# Load environment variables from .env file
load_dotenv()

# Database connection parameters from .env
db_config = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME')
}

# SQL queries to select data from WordPress tables
queries = {
    "posts": """
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
    """,
    "pages": """
    SELECT 
        p.ID AS page_id,
        p.post_title,
        p.post_content,
        p.post_date,
        u.display_name AS author_name
    FROM 
        wp_posts p
    LEFT JOIN 
        wp_users u ON p.post_author = u.ID
    WHERE 
        p.post_type = 'page' AND p.post_status = 'publish';
    """,
    "products": """
    SELECT 
        p.ID AS product_id,
        p.post_title,
        p.post_content,
        p.post_date,
        u.display_name AS author_name,
        pm.meta_value AS price
    FROM 
        wp_posts p
    LEFT JOIN 
        wp_users u ON p.post_author = u.ID
    LEFT JOIN 
        wp_postmeta pm ON p.ID = pm.post_id AND pm.meta_key = '_price'
    WHERE 
        p.post_type = 'product' AND p.post_status = 'publish';
    """,
    "contacts": """
    SELECT 
        id AS contact_id,
        name,
        email,
        message,
        created_at
    FROM 
        wp_contact_form;  -- Adjust this table name based on your setup
    """
}

# Function to check if a table exists
def table_exists(cursor, table_name):
    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    return cursor.fetchone() is not None

# Function to clean HTML content
def clean_html(text):
    return html.unescape(pd.Series(text).str.replace(r'<.*?>', '', regex=True)).tolist()

# Function to execute query and return DataFrame
def fetch_data(query):
    return pd.read_sql(query, connection)

try:
    # Connect to the database
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    # Initialize a list to hold all DataFrames
    all_data = []

    # Check and fetch posts
    if table_exists(cursor, 'wp_posts'):
        posts_df = fetch_data(queries["posts"])
        posts_df['post_content'] = clean_html(posts_df['post_content'])
        all_data.append(posts_df)

    # Check and fetch pages
    if table_exists(cursor, 'wp_posts'):
        pages_df = fetch_data(queries["pages"])
        pages_df['post_content'] = clean_html(pages_df['post_content'])
        all_data.append(pages_df)

    # Check and fetch products
    if table_exists(cursor, 'wp_posts') and table_exists(cursor, 'wp_postmeta'):
        products_df = fetch_data(queries["products"])
        products_df['post_content'] = clean_html(products_df['post_content'])
        all_data.append(products_df)

    # Check and fetch contacts
    if table_exists(cursor, 'wp_contact_form'):  # Adjust based on your setup
        contacts_df = fetch_data(queries["contacts"])
        all_data.append(contacts_df)

    # Concatenate all DataFrames into one
    combined_df = pd.concat(all_data, ignore_index=True)

    # Save combined DataFrame to a single CSV file
    combined_df.to_csv('wordpress_data.csv', index=False)
    print("Data exported successfully to wordpress_data.csv.")

except mysql.connector.Error as err:
    print(f"Error: {err}")

finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
