import streamlit as st
import datetime
import pandas as pd
import nfl_data_py as nfl
from PIL import Image
import psycopg2
from psycopg2 import Error
from sqlalchemy import create_engine
import pytz
import hashlib


# Vars
# USER = st.secrets["USER"]
# PASSWORD = st.secrets["PASSWORD"]
# DATABASE = st.secrets["DATABASE"]
# HOST = st.secrets["HOST"]

USER = "postgres"
PASSWORD = "Barca2011!"
DATABASE_NAME = "nfl_weekly_picks"
HOST = "nfl-weekly-picks.cdd5mq5zdhsy.eu-west-2.rds.amazonaws.com"
PORT = 5432


NON_UNIQUE_USERNAME = "Username already exists. Please try again with a different one"
NON_UNIQUE_EMAIL = "Email already exists. Please try again with a different one"
USER_CREATION_SUCCESS_MESSAGE = "Successfully executed the command"
WEEK_SCHEDULE_COLUMN_LIST = ["game_id", "week", "gameday", "weekday", "gametime", "away_team",
                             "home_team",
                           "away_rest", "home_rest", "spread_line", "stadium"]
WEEK_COLUMN = "week"
TEXT_AT_SIGN = "@"
TEXT_DASH_SIGN = "-"
TEXT_REST = "rested"
TEXT_DAYS = "days"
TEXT_SPREAD = "Spread is"
TEXT_SPACE = " "
TEAM_LOGO_LOCATIONS_DF = pd.read_csv(
    r"https://github.com/JaumeClave/nfl_weekly_picks/blob/master/data/processed/team_logo_file_locations.csv?raw=true")
START_HEADER_CENTERED_HTML = "<h1 style='text-align: center;'>"
END_HEADER_HTML_HTML = "</h1>"
START_PARAGRAPH_HTML = "<p style='text-align: center;'>"
END_PARAGRAPH_HTML = "</p>"


@st.cache(allow_output_mutation=True)
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
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
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


@st.cache(show_spinner=True)
def make_yearly_schedule(year):
    """
    Function returns a dataframe containing the provided years NFL schedule
    :param year: year of schedule desired
    :return: Pandas Dataframe
    """
    yearly_schedule_2022_df = nfl.import_schedules([2022])
    yearly_schedule_2022_df["gameday"] = pd.to_datetime(yearly_schedule_2022_df["gameday"]).dt.date
    return yearly_schedule_2022_df


def make_nfl_game_scores_df(nfl_schedule_df):
    """
    Function makes a dataframe containing key data for NFL games which have been played
    :return: Pandas DataFrame
    """
    games_with_scores_df = nfl_schedule_df[(nfl_schedule_df["away_score"].notna()) & (nfl_schedule_df["home_score"].notna())]
    games_with_scores_df = games_with_scores_df[["game_id", "week", "away_team", "away_score", "home_team", "home_score"]]
    return games_with_scores_df


def make_insert_into_nfl_game_scores_2022_table(game_id, week, away_team, away_score, home_team, home_score):
    """
    Function inserts the a game_id and its features into the nfl_game_scores_2022 table
    :param game_id: game id key
    :param week: int - week number
    :param away_team: game away team
    :param away_score: game away score
    :param home_team: game home team
    :param home_score: game away score
    :return: None
    """
    query = """
                 INSERT INTO nfl_game_scores_2022 (game_id, week, away_team, away_score, home_team, home_score)
                 VALUES (%s, %s, %s, %s, %s, %s)
                 ;
            """
    data_tuple = (game_id, week, away_team, away_score, home_team, home_score)
    cursor_execute_tuple(query, data_tuple)
    return None


def pipeline_make_insert_into_nfl_game_scores_2022_table(nfl_schedule_df):
    """
    Function pipelines the process required to add a game_id along with relevant data to the nfl_game_scores_2022 table
    :return: None
    """
    nfl_games_with_scores_df = make_nfl_game_scores_df(nfl_schedule_df)
    for index, row in nfl_games_with_scores_df.iterrows():
        make_insert_into_nfl_game_scores_2022_table(row["game_id"], row["week"],
                                                    row["away_team"], row["away_score"],
                                                    row["home_team"], row["home_score"])
    return nfl_games_with_scores_df


def make_current_nfl_week_number(yearly_schedule):
    """
    Function finds the current NFL week by filtering for games/weeks that are less than today's date
    :param yearly_schedule: Dataframe containing the provided years NFL schedule
    :return: Current NFL week - int
    """
    current_date = datetime.datetime.now().date()
    dates_less_than_today = yearly_schedule[yearly_schedule["gameday"] <= current_date]
    current_week = dates_less_than_today["week"].max()
    return current_week


def make_week_schedule(yearly_schedule_df, week_number):
    """
    Function returns a dataframe containing home & away team & rest, kickoff time and day and spread
    :param week_number:
    :return:
    """
    yearly_week_schedule_df = yearly_schedule_df[yearly_schedule_df[WEEK_COLUMN] == week_number]
    week_schedule_df = yearly_week_schedule_df[WEEK_SCHEDULE_COLUMN_LIST]
    week_schedule_df.sort_values(["gameday", "gametime"], axis=0, ascending=True, inplace=True)
    return week_schedule_df


def make_list_of_dicts_with_gameday_info(weekly_schedule):
    """
    Function creates a dictionary containing key gameday information for each weekly matchup and appends it to a list
    :param weekly_schedule: Dataframe containing a weeks NFL schedule
    :return: List containing a dictionary of each matchup
    """
    game_info_list_of_dicts = list()
    game_info_dict = dict()
    for index, row in week_schedule_df.iterrows():
        game_info_dict["game_id"] = row["game_id"]
        game_info_dict["away_team"] = row["away_team"]
        game_info_dict["home_team"] = row["home_team"]
        game_info_dict["weekday"] = row["weekday"]
        game_info_dict["gametime"] = row["gametime"]
        game_info_dict["stadium"] = row["stadium"]
        game_info_dict["away_rest"] = row["away_rest"]
        game_info_dict["home_rest"] = row["home_rest"]
        game_info_dict["spread_line"] = row["spread_line"]
        game_info_list_of_dicts.append(game_info_dict.copy())
    return game_info_list_of_dicts


def make_matchup_texts(game_info):
    """
    Function makes the Away vs Home text, Gameday/Time/Place text and Away/Home Rest and Spread text
    :param game_info: Dictionary containing gameday information text
    :return: three text strings
    """
    game_id = game_info["game_id"]
    away_vs_home_text = game_info["away_team"] + TEXT_SPACE + TEXT_AT_SIGN + TEXT_SPACE + \
                        game_info["home_team"]
    game_day_time_place_text = game_info["weekday"] + TEXT_SPACE + TEXT_DASH_SIGN + TEXT_SPACE +\
                               game_info["gametime"] + TEXT_SPACE + "ET" + TEXT_SPACE + \
                               TEXT_AT_SIGN + TEXT_SPACE + game_info["stadium"]
    away_home_rest_and_spread_text = game_info["away_team"] + TEXT_SPACE + TEXT_REST + \
                                     TEXT_SPACE + str(game_info["away_rest"]) + TEXT_SPACE + \
                                     TEXT_DAYS + TEXT_SPACE + TEXT_DASH_SIGN + TEXT_SPACE + \
                                     game_info["home_team"] + TEXT_SPACE + TEXT_REST + \
                                     TEXT_SPACE + str(game_info["home_rest"]) + TEXT_SPACE + \
                                     TEXT_DAYS + TEXT_SPACE + TEXT_DASH_SIGN + TEXT_SPACE + \
                                     TEXT_SPREAD + TEXT_SPACE + str(game_info["spread_line"])
    return game_id, away_vs_home_text, game_day_time_place_text, away_home_rest_and_spread_text


def make_single_matchup_list(game_id, away_vs_home_text, game_day_time_place_text, away_home_rest_and_spread_text):
    """
    Function appends the three texts into a single matchup list
    :param away_vs_home_text: Away vs Home text
    :param game_day_time_place_text: Gameday/Time/Place text
    :param away_home_rest_and_spread_text: Away/Home Rest and Spread text
    :return: List holding all three texts
    """
    away_vs_home_list = list()
    game_day_time_place_list = list()
    away_home_rest_and_spread_list = list()
    away_vs_home_list.append(away_vs_home_text)
    game_day_time_place_list.append(game_day_time_place_text)
    away_home_rest_and_spread_list.append(away_home_rest_and_spread_text)
    single_matchup_list = [game_id] + away_vs_home_list + game_day_time_place_list + away_home_rest_and_spread_list
    away_vs_home_list = list()
    game_day_time_place_list = list()
    away_home_rest_and_spread_list = list()
    return single_matchup_list


def make_list_of_matchups_list(game_info_list_of_dicts):
    """
    Function outputs a lists of lists containing gameday text information for each matchup
    :param game_info_list_of_dicts: List containing a dictionary of each matchup
    :return: Lists of lists containing gameday text information for each matchup
    """
    all_matchup_list = list()
    for i in range(len(game_info_list_of_dicts)):
        game_id, away_vs_home_text, game_day_time_place_text, away_home_rest_and_spread_text = make_matchup_texts(game_info_list_of_dicts[i])
        single_matchup_list = make_single_matchup_list(game_id, away_vs_home_text, game_day_time_place_text, away_home_rest_and_spread_text)
        all_matchup_list.append(single_matchup_list)
    return all_matchup_list


def pipeline_make_matchup_text_lists(weekly_schedule):
    """
    Function pipelines the process required to output a lists of lists containing gameday text information for each matchup
    :param weekly_schedule: Dataframe containing a weeks NFL schedule
    :return: Lists of lists containing gameday text information for each matchup
    """
    game_info_list_of_dicts = make_list_of_dicts_with_gameday_info(weekly_schedule)
    all_matchup_list = make_list_of_matchups_list(game_info_list_of_dicts)
    return all_matchup_list


def make_team_logo_image(team_acronym):
    """
    Function creates and Pillow image object from a specified file path loaded up from a dataframe
    :param team_acronym: Acronym of team name
    :return: Rendered image of team logo
    """
    image_location = TEAM_LOGO_LOCATIONS_DF[TEAM_LOGO_LOCATIONS_DF["team"] == team_acronym]["picture_location"].iloc[0]
    logo = Image.open(image_location)
    return logo


def add_values_in_dict(dictionary, key, list_of_values):
    """
    Function checks if key is present in the dictionary, if not, it creates key and extends the dictionary with list of value provided
    :param dictionary: Dictionary to be added to
    :param key: Key to be extended into dictionary
    :param list_of_values: List of values to be extended into dictionary
    :return: Dictionary
    """
    if key not in dictionary:
        dictionary[key] = list()
    dictionary[key].extend(list_of_values)
    return dictionary


def make_gameday_gameid_home_away(all_matchup_list):
    """
    Function makes the variables for game id, home team and away team
    :param all_matchup_list: Lists of lists containing gameday text information for each matchup
    :return: Variables for game id, home team and away team
    """
    game_daytime = all_matchup_list[i][2].split(" @ ")[0]
    game_id = all_matchup_list[i][0]
    home_team = all_matchup_list[i][1].split(" @ ")[1]
    away_team = all_matchup_list[i][1].split(" @ ")[0]
    return game_daytime, game_id, home_team, away_team


def make_weekly_picks_df(weekly_picks_dict, user_id):
    """
    Function creates a dataframe containing user_id and the users respective weekly matchup winning picks
    :param weekly_picks_dict: Dictionary containing game_id as a key and the winning pick as a value
    :param user_id: ID of user
    :return: Dataframe containing user id and weekly winning picks
    """
    weekly_picks_df = pd.DataFrame(weekly_picks_dict).T.reset_index()
    weekly_picks_df.columns = ["game_id", "winning_pick"]
    weekly_picks_df["user_id"] = user_id
    weekly_picks_df["user_id_game_id"] = weekly_picks_df["user_id"].astype(str) + "_" + weekly_picks_df["game_id"]
    return weekly_picks_df


def make_gamedaytime_timedelta(week_schedule_df, game_id):
    """
    Function makes a timestamp from a game_id
    :param week_schedule_df: Dataframe containing a weeks NFL schedule
    :param game_id: game_id key
    :return: Timestamp
    """
    gametime_df = week_schedule_df[["game_id", "gameday", "gametime", "weekday"]]
    gametime_df["gamedaytime"] = gametime_df["gameday"].astype(str) + " " + gametime_df["gametime"]
    gametime_df["gamedaytime"] = pd.to_datetime(gametime_df["gamedaytime"])
    timestamp = gametime_df[gametime_df["game_id"] == game_id]["gamedaytime"].iloc[0]
    return timestamp


def make_time_to_game(timestamp):
    """
    Function finds the difference between two times
    :param timestamp: Timestamp object
    :return: Timedelta difference between now and gametime
    """
    tz = pytz.timezone('US/Eastern')
    time_now = datetime.datetime.now(tz=tz).replace(tzinfo=None)
    timedelta_difference = timestamp - time_now
    return timedelta_difference


def make_days_hours_minutes(timedelta):
    """
    Function returns a tuple containing the days, hours and minutes from a Timedelta object
    :param timedelta: Timedelta object
    :return: dtuple containing the days, hours and minutes
    """
    days = timedelta.days
    hours = timedelta.seconds//3600
    minutes = (timedelta.seconds//60)%60
    if days < 0:
        days = 0
        hours = 0
        minutes = 0
        return days, hours, minutes
    return days, hours, minutes


def make_countdown_text(days, hours, minutes):
    """
    Function creates the text output to be displayed during the countdown
    :param days: Int - days left
    :param hours: Int - hours left
    :param minutes: Int - minutes left
    :return: Countdown text
    """
    if days == 0 and hours == 0 and minutes == 0:
        countdown_text = "Game has started - You can't make a pick post-kickoff"
    else:
        if days == 1:
            day_var = "day"
        else:
            day_var = "days"
        if hours == 1:
            hour_var = "hour"
        else:
            hour_var = "hours"
        if minutes == 1:
            minute_var = "minute"
        else:
            minute_var = "minutes"
        days_text = "{} {}".format(days, day_var)
        hours_text = "{} {}".format(hours, hour_var)
        minutes_text = "{} {}".format(minutes, minute_var)
        countdown_text = days_text + " : " + hours_text + " : " + minutes_text + " to make your picks..."
    return countdown_text


def pipeline_make_countdown_text(week_schedule_df, game_id):
    """
    Function pipelines the process required to output the countdown text for a game
    :param week_schedule_df: Dataframe containing a weeks NFL schedule
    :param game_id: game_id key
    :return: Countdown text
    """
    game_timestamp = make_gamedaytime_timedelta(week_schedule_df, game_id)
    timedelta_difference = make_time_to_game(game_timestamp)
    days, hours, minutes = make_days_hours_minutes(timedelta_difference)
    countdown_text = make_countdown_text(days, hours, minutes)
    return days, hours, minutes, countdown_text


def make_current_picks_df(user_id):
    """
    Function queries the user_weekly_picks table and returns a Pandas DataFrame for the specified users data
    :param user_id: user_id key
    :return: Dataframe
    """
    engine = create_engine("postgresql+psycopg2://" + USER + ":" + PASSWORD + "@" + HOST + "/" + DATABASE)
    query = """
         SELECT user_id, game_id, winning_pick
         FROM user_weekly_picks
         WHERE user_id=%(user_id)s
         ;
         """
    current_picks_df = pd.read_sql_query(query, con=engine, params={"user_id": user_id})
    return current_picks_df


def make_insert_into_weekly_picks_table(user_id_game_id, user_id, game_id, winning_pick, timestamp):
    """
    Function inserts the user_weekly_picks into the weekly_picks table. It uses the INSERT INTO along with the ON CONFLICT DO UPDATE SET clause in order to update the user_weekly_picks field when the user_id_game_id is already in that table
    :param user_id_game_id: user_id and game_id key
    :param user_id: user_id key
    :param game_id: game_id key
    :param winning_pick: winning team pick
    :param timestamp: datetime
    :return: None
    """
    query = """
                 INSERT INTO user_weekly_picks (user_id_game_id, user_id, game_id, winning_pick, timestamp_added)
                 VALUES (%s, %s, %s, %s, %s)
                 ON CONFLICT (user_id_game_id) DO UPDATE SET
                 (user_id, game_id, winning_pick, timestamp_added) = (EXCLUDED.user_id, EXCLUDED.game_id, EXCLUDED.winning_pick, EXCLUDED.timestamp_added);
            """
    data_tuple = (user_id_game_id, user_id, game_id, winning_pick, timestamp)
    cursor_execute_tuple(query, data_tuple)
    return None


def make_logical_insert_into_weekly_picks_table(weekly_picks_df):
    """
    Function holds the logic used to insert matchups when they don't exist and update them when they differ from their current value
    :param weekly_picks_df: Dataframe containing user id and weekly winning picks
    :return:
    """
    current_picks_df = make_current_picks_df(user_id)
    timestamp = datetime.datetime.now()
    for index, row in weekly_picks_df.iterrows():
        temp_df = current_picks_df[current_picks_df["game_id"] == row["game_id"]]
        if len(temp_df) == 0: # If not game_id record exists then upload matchup
            make_insert_into_weekly_picks_table(row["user_id_game_id"], row["user_id"], row["game_id"], row["winning_pick"], timestamp)
        elif temp_df["winning_pick"].iloc[0] == row["winning_pick"]: # If game_id exists and winning pick is unchanged then pass
            pass
        elif temp_df["winning_pick"].iloc[0] != row["winning_pick"]: # If game_id exists and winning pick is different then upload matchup
            make_insert_into_weekly_picks_table(row["user_id_game_id"], row["user_id"], row["game_id"], row["winning_pick"], timestamp)
    return None


def pipeline_make_insert_into_weekly_picks_table(weekly_picks_dict, user_id):
    """
    Function pipelines the process required to insert the weekly_picks_df into the weekly_picks table
    :param weekly_picks_dict: Dictionary containing game_id as a key and the winning pick as a value
    :param user_id: ID of user
    """
    weekly_picks_df = make_weekly_picks_df(weekly_picks_dict, user_id)
    make_logical_insert_into_weekly_picks_table(weekly_picks_df)
    return None


##################################### STREAMLIT UI ###########################################


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
            st.experimental_rerun()
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


def make_game_day_and_countdown_ui(game_daytime):
    """
    Function creates the logic and UI for matchup day and countdown
    """
    if game_daytime not in game_day_list:
        c1, c2 = st.columns((1, 3))
        with c1:
            game_day = game_daytime.split(" - ")[0]
            st.subheader(game_day)
        with c2:
            st.write("")
            countdown_text = pipeline_make_countdown_text(week_schedule_df, game_id)[3]
            st.text(countdown_text)
        game_day_list.append(game_daytime)
    days, hours, minutes = pipeline_make_countdown_text(week_schedule_df, game_id)[0:3]
    if days == 0 and hours == 0 and minutes == 0: # Logic returning True/False if game has
        # started. Used to disable checkboxes
        return True
    else:
        return False


def make_column1_ui(game_started_flag):
    """
    Function creates the logic and UI for column1
    """
    st.subheader("")
    if st.checkbox(away_team + " to win!", disabled=game_started_flag):
        add_values_in_dict(weekly_picks_dict, game_id, [away_team])
        checkbox_select_double_win_list.append(1)
    logo_away = make_team_logo_image(away_team)
    st.image(logo_away, use_column_width=True)


def make_column2_ui():
    """
    Function creates the logic and UI for column2
    """
    st.markdown("{open}{text}{close}".format(open=START_HEADER_CENTERED_HTML,
                                             text=all_matchup_list[i][1],
                                             close=END_HEADER_HTML_HTML), unsafe_allow_html=True)
    st.markdown("{open}{text}{close}".format(open=START_PARAGRAPH_HTML,
                                             text=all_matchup_list[i][2],
                                             close=END_PARAGRAPH_HTML), unsafe_allow_html=True)
    st.markdown("{open}{text}{close}".format(open=START_PARAGRAPH_HTML,
                                             text=all_matchup_list[i][3],
                                             close=END_PARAGRAPH_HTML), unsafe_allow_html=True)


def make_column3_ui(game_started_flag):
    """
    Function creates the logic and UI for column3
    """
    st.subheader("")
    if st.checkbox(home_team + " to win!", disabled=game_started_flag):
        add_values_in_dict(weekly_picks_dict, game_id, [home_team])
        checkbox_select_double_win_list.append(1)
    logo_home = make_team_logo_image(home_team)
    st.image(logo_home, use_column_width=True)


def make_warning_two_team_matchup_win_selected():
    """
    Function checks if a matchup has had two winners selected and raises a Streamlit warning if
    it does
    """
    if sum(checkbox_select_double_win_list) == 2: # Check if none or one button have been selected
        st.warning("Both {away} and {home} can't win! Please select only one team.".format(
            away=away_team, home=home_team))


def make_submit_weekly_picks_button():
    """
    Function creates the logic and UI for the Submit Weekly Picks button
    :return:
    """
    wins_selected_per_matchup_dict = {k: len(v) for k, v in weekly_picks_dict.items()}
    try:
        if max(wins_selected_per_matchup_dict.values()) == 1:
            if st.button("Submit Picks!"):
                pipeline_make_insert_into_weekly_picks_table(weekly_picks_dict, user_id)
                st.success("Submitted")
    except ValueError:
        pass


######################################### RUN #######################################


# Connect to DB
con, cursor = connect_to_postgres_database(USER, PASSWORD, DATABASE_NAME, HOST, port="5432")


try:
    # User ID
    user_id = st.session_state["user_id"]

    # Streamlit - Title
    st.markdown("{open}NFL Weekly Picks 🏈{close}".format(open=START_HEADER_CENTERED_HTML,
                                                  close=END_HEADER_HTML_HTML), unsafe_allow_html=True)


    # Get yearly schedule
    with st.spinner('Getting the 2022 NFL schedule...'):
        yearly_schedule_2022_df = make_yearly_schedule(2022)
        pipeline_make_insert_into_nfl_game_scores_2022_table(yearly_schedule_2022_df)

    # Get current NFL week number
    current_nfl_week_number = make_current_nfl_week_number(yearly_schedule_2022_df)


    c1, c2, c3, c4, c5 = st.columns(5)
    with c3:
        # Get and show current week schedule
        week_number = st.number_input("NFL Week Number", min_value=1, max_value=18,
                                      value=current_nfl_week_number, step=1)


    week_schedule_df = make_week_schedule(yearly_schedule_2022_df, week_number)
    all_matchup_list = pipeline_make_matchup_text_lists(week_schedule_df)
    st.markdown("""---""")


    game_day_list = list()
    weekly_picks_dict = dict()
    # Streamlit - Display matchups
    for i in range(len(all_matchup_list)):
        game_day, game_id, home_team, away_team = make_gameday_gameid_home_away(all_matchup_list)
        game_started_flag = make_game_day_and_countdown_ui(game_day)
        checkbox_select_double_win_list = list()
        c1, c2, c3 = st.columns((1, 3, 1))
        with c1:
            make_column1_ui(game_started_flag)
        with c2:
            make_column2_ui()
        with c3:
            make_column3_ui(game_started_flag)
        make_warning_two_team_matchup_win_selected()
        st.markdown("""---""")

    c1, c2, c3, c4, c5 = st.columns(5)
    with c3:
        make_submit_weekly_picks_button()
except KeyError:
    login_and_signup_ui_app()
