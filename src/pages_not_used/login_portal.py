import hashlib
from datetime import datetime

import psycopg2
import streamlit as st
from psycopg2 import Error


# Variables
USER = "postgres"
PASSWORD = "Barca2011"
DATABASE = "nfl_weekly_picks"
HOST = "localhost"
NON_UNIQUE_USERNAME = "Username already exists. Please try again with a different one"
NON_UNIQUE_EMAIL = "Email already exists. Please try again with a different one"
USER_CREATION_SUCCESS_MESSAGE = "Successfully executed the command"


# Functions
# @st.cache(allow_output_mutation=True)
def connect_to_postgres_database(user, password, database, host="127.0.0.1", port="5432"):
    """
    Function connects to a database and returns the cursor object
    :param user: database username
    :param password: database password
    :param database: database name
    :param host: server location
    :param port: listening port
    :return: psycopg2 cursor object
    """
    try:
        con = psycopg2.connect(user=user,
                               password=password,
                               database=database,
                               host=host,
                               port=port)
        cursor = con.cursor()
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    return con, cursor


def cursor_execute_tuple(command, data_tuple):
    """
    Function uses the cursor object to execute a command with a tuple pair. It commits and rollsback if error
    :param command: SQL query to be executed
    :param data_tuple: data pairing for SQL query variables
    :return:
    """
    try:
        cursor.execute(command, data_tuple)
        con.commit()
        print("Successfully executed the command")
    except:
        con.rollback()
        print("Could not successfully execute the command")
    return None


def make_check_for_unique_username(username_value):
    """
    Function checks if the username provided is unique in username column in dashboard_user table
    :param username_value: username
    :return: False - if not unique
    """
    insert_command = """SELECT username FROM users
                        WHERE username = %s
                        LIMIT 1;"""
    cursor.execute(insert_command, [username_value])
    returned_value = cursor.fetchall()
    if len(returned_value) != 0:
        return False


def make_check_for_unique_email(email_value):
    """
    Function checks if the email provided is unique in email column in the dashboard_user table
    :param email_value: email
    :return: False - if not unique
    """
    insert_command = """SELECT email FROM users
                        WHERE email = %s
                        LIMIT 1;"""
    cursor.execute(insert_command, [email_value])
    returned_value = cursor.fetchall()
    if len(returned_value) != 0:
        return False


# Hashing
def make_hashes(password):
    """
    Function takes a given string and returns a hashed string
    :param password: user password
    :return: hashed password
    """
    return hashlib.sha256(str.encode(password)).hexdigest()


def check_hashes(password, hashed_text):
    """
    Function checks if the provided password, when hashed, matches the hashed value for the password
    :param password: user password
    :param hashed_text: hashed password
    :return: False/True
    """
    if make_hashes(password) == hashed_text:
        return hashed_text
    return False


def make_date_time():
    """
    Function makes the current date and time
    :return: current date and time
    """
    date_created = datetime.today().date()
    time_created = datetime.now().time().strftime("%H:%M:%S")
    return date_created, time_created


# Insert user data into user table
def insert_user_in_user_table(username, password, email):
    """
    Function attempts to insert a user into the dashboard_user table. It first checks to see if unique values exist for username and email
    :param username: username
    :param password: user password
    :param email: email
    :return: None
    """
    if make_check_for_unique_username(username) == False:
        return NON_UNIQUE_USERNAME
    elif make_check_for_unique_email(email) == False:
        return NON_UNIQUE_EMAIL
    else:
        hashed_password = make_hashes(password)
        date_created, time_created = make_date_time()
        insert_command = """INSERT INTO users
                      (username, password, email, date_created, time_created)
                      VALUES (%s, %s, %s, %s, %s);"""
        data_tuple = (username, hashed_password, email, date_created, time_created)
        cursor_execute_tuple(insert_command, data_tuple)
    return None


# Make username/password check
def make_username_password_login_check(username, password):
    """
    Function checks the dashboard_user table to see if the username and password provided on login matches a registered user
    :param username: username
    :param password: user password
    :return: True/False
    """
    hashed_password = make_hashes(password)
    insert_command = """SELECT * FROM users
                        WHERE username = %s AND password = %s;"""
    cursor.execute(insert_command, [username, hashed_password])
    returned_value = cursor.fetchall()
    if len(returned_value) != 0:
        return True
    else:
        return False


def make_id_from_username(username):
    """
    Function checks the dashboard_user table and returns the user_id by referencing a username
    :param username: username
    :return: user_id
    """
    insert_command = """SELECT user_id FROM users
                        WHERE username = %s;"""
    cursor.execute(insert_command, [username])
    returned_value = cursor.fetchall()
    return returned_value[0][0]


########################################### STREAMLIT ################################################


# Connect to DB
con, cursor = connect_to_postgres_database(USER, PASSWORD, DATABASE, HOST, port="5432")


def login_and_signup_ui_app():
    """
    Function to render the authentication.py page via the app.py file
    """
    # Sidebar navigation
    menu = ["Login", "SignUp"]
    choice = st.sidebar.selectbox("Menu", menu)
    # Login UI
    if choice == "Login":
        st.subheader("Login")
        username = st.text_input("User Name")
        password = st.text_input("Password", type='password')
        # Pressing Login
        if st.button("Login"):
            result = make_username_password_login_check(username, password)
            if result:
                st.success("Logged In as {}".format(username))
                # Initialization Session State
                id = make_id_from_username(username)
                if "user_id" not in st.session_state:
                    st.session_state["user_id"] = id
            else:
                st.error("Incorrect Username/Password")
    # SignUp UI
    elif choice == "SignUp":
        st.subheader("Create New Account")
        new_user = st.text_input("Username")
        new_email = st.text_input("Email")
        new_password = st.text_input("Password", type='password')
        # Pressing Signup
        if st.button("Signup"):
            user_creation_statement = insert_user_in_user_table(new_user, new_password, new_email)
            # Check for unique username/email
            if user_creation_statement == NON_UNIQUE_USERNAME:
                st.error(NON_UNIQUE_USERNAME)
            elif user_creation_statement == NON_UNIQUE_EMAIL:
                st.error(NON_UNIQUE_EMAIL)
            elif user_creation_statement is None:
                st.success("You have successfully created a valid Account")
                st.info("Go to Login Menu to login")
    return None

app()