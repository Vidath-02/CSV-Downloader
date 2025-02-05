from flask import Flask, request, render_template, send_file, jsonify
import mysql.connector
import csv
import io
import logging
import re

#Developed by Vidath Kumarasiri


# Initialize the Flask application
app = Flask(__name__, template_folder='/home/vidath/Documents/Pythonproject/templates')

# Configure app settings
app.config['SECRET_KEY'] = 'your_secret_key'  # Secret key for sessions and CSRF protection
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # Limit file upload size to 16MB

# Set up logging for better error tracking
logging.basicConfig(level=logging.DEBUG)

# Caching static files for 30 days for better performance
@app.after_request
def set_cache_headers(response):
    if response.status_code == 200 and 'static' in request.path:
        response.cache_control.public = True
        response.cache_control.max_age = 30 * 24 * 60 * 60  # Cache for 30 days
    return response

# Custom handler for 404 errors
@app.errorhandler(404)
def handle_page_not_found(error):
    logging.error(f"Page not found: {error}")
    return render_template('404.html'), 404

# Home route to render the main page
@app.route('/')
def home():
    return render_template('index.html')

# Function to sanitize SQL queries and ensure proper structure
def sanitize_sql(query, table_name, columns):
    """
    This function sanitizes and builds a SQL query:
    - If no query is provided, it constructs a default query using the table name and columns.
    - If a query is provided, it checks for correct usage of ORDER BY and LIMIT.
    """
    if query:
        # Remove any trailing semicolons
        query = query.strip().rstrip(';')
    else:
        # Default query if no custom query is provided
        query = f"SELECT {columns if columns else '*'} FROM {table_name}"

    # Validate 'ORDER BY' clause to ensure it uses a valid column (e.g., 'created_date')
    if 'ORDER BY' in query:
        match = re.search(r'ORDER BY (\w+)', query)
        if match:
            column = match.group(1)
            if column not in ['created_date']:  # You can add more column validations if needed
                raise ValueError(f"Invalid column name in ORDER BY: {column}")

    # Handle the 'LIMIT' clause
    if 'LIMIT' in query:
        limit_index = query.find('LIMIT')
        query = query[:limit_index].strip() + ' ' + query[limit_index:].strip()

    return query

# Route for downloading data as CSV
@app.route('/download_csv', methods=['POST'])
def download_csv():
    # Extract form data from the frontend
    data_source_url = request.form.get('data_source_url')
    username = request.form.get('username')
    password = request.form.get('password')
    table_name = request.form.get('table_name')
    columns = request.form.get('columns')  # Optional: Specific columns
    sql_query = request.form.get('sql_query')  # Optional: Custom SQL query

    # Check if all required fields are present
    if not data_source_url or not username or not password or not table_name:
        return jsonify({'error': 'Missing required parameters!'}), 400

    # Parsing and validating the data source URL
    try:
        if not data_source_url.startswith('jdbc:mysql://'):
            return jsonify({'error': 'Invalid data source URL format!'}), 400

        stripped_url = data_source_url.replace('jdbc:mysql://', '')
        host_port, database = stripped_url.split('/')
        host, port = host_port.split(':')
    except Exception as e:
        logging.error(f"Error parsing data source URL: {e}")
        return jsonify({'error': 'Invalid data source URL format!'}), 400

    # Connecting to the MySQL database
    try:
        connection = mysql.connector.connect(
            host=host,
            port=port,
            user=username,
            password=password,
            database=database
        )
        cursor = connection.cursor(dictionary=True)

        # Sanitize and finalize the SQL query
        query = sanitize_sql(sql_query, table_name, columns)
        logging.debug(f"Finalized query: {query}")

        # Execute the query
        cursor.execute(query)
        rows = cursor.fetchall()

        if not rows:
            return jsonify({'error': 'No data found with the specified query!'}), 404

        # Create a CSV file in memory from the query results
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
        output.seek(0)

        # Return the CSV as a downloadable file
        return send_file(
            io.BytesIO(output.getvalue().encode()),
            mimetype='text/csv',
            as_attachment=True,
            download_name=f"{table_name}.csv"
        )
    except mysql.connector.Error as err:
        logging.error(f"Database error: {err}")
        return jsonify({'error': f"Database error: {err}"}), 500
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return jsonify({'error': f"Unexpected error: {e}"}), 500
    finally:
        # Ensure to close the cursor and connection properly
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()

# Run the Flask application in debug mode
if __name__ == '__main__':
    app.run(debug=True)

