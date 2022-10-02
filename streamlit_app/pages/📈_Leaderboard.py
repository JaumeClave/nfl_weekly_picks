import streamlit as st
import datetime
import pandas as pd
import nfl_data_py as nfl
from PIL import Image
import psycopg2
from psycopg2 import Error
from sqlalchemy import create_engine


# Vars
USER = st.secrets["USER"]
PASSWORD = st.secrets["PASSWORD"]
DATABASE_NAME = st.secrets["DATABASE_NAME"]
HOST = st.secrets["HOST"]
PORT = st.secrets["PORT"]


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


def make_database_games_with_scores_df():
    """
    Function queries the nfl_game_scores_2022 table and returns a Pandas DataFrame
    :return: Dataframe
    """
    engine = create_engine("postgresql+psycopg2://" + USER + ":" + PASSWORD + "@" + HOST + "/" +
                           DATABASE_NAME)
    query = """
         SELECT week, away_team, away_score, home_team, home_score
         FROM nfl_game_scores_2022
         ;
         """
    database_games_with_scores_df = pd.read_sql_query(query, con=engine)
    return database_games_with_scores_df


def make_games_with_scores_df():
    """
    Function creates a dataframe with a users chosen games and a flag for correct matchup pick
    :param user_id: user id key
    :return: Dataframe
    """
    engine = create_engine("postgresql+psycopg2://" + USER + ":" + PASSWORD + "@" + HOST + "/" +
                           DATABASE_NAME)
    query = """
        WITH nfl_game_scores_2022 AS (
            SELECT week, game_id,
                CASE
                    WHEN away_score > home_score THEN away_team
                    WHEN away_score < home_score THEN home_team
                    WHEN away_score = home_score THEN 'TIE'
                END AS winning_team
            FROM nfl_game_scores_2022
        ),
            user_weekly_picks AS (
            SELECT user_id_game_id, user_id, game_id, winning_pick
            FROM user_weekly_picks
        ),
            left_join_above AS (
            SELECT usr.user_id_game_id, usr.user_id, nfl.game_id, nfl.week,
                CASE
                    WHEN usr.winning_pick = nfl.winning_team THEN 1
                    WHEN usr.winning_pick != nfl.winning_team THEN 0
                END AS correct_pick_flag
            FROM nfl_game_scores_2022 nfl
            LEFT JOIN user_weekly_picks usr
                ON nfl.game_id = usr.game_id
            WHERE usr.user_id_game_id IS NOT NULL
        )
         SELECT * FROM left_join_above ;
         """
    database_games_with_scores_df = pd.read_sql_query(query, con=engine)
    return database_games_with_scores_df


def make_insert_into_user_winning_picks_table(user_id_game_id, user_id, game_id, week, correct_pick_flag):
    """
    Function inserts picks into the winning picks table
    :param user_id_game_id: user and game id key
    :param user_id: user_id key
    :param game_is:  game_id key
    :param week: int - week
    :param correct_pick_flag: boolean (1, 0)
    :return: None
    """
    query = """
                 INSERT INTO user_winning_picks (user_id_game_id, user_id, game_id, week, correct_pick_flag)
                 VALUES (%s, %s, %s, %s, %s)
                 ;
            """
    data_tuple = (user_id_game_id, user_id, game_id, week, correct_pick_flag)
    cursor_execute_tuple(query, data_tuple)
    return None


def pipeline_make_insert_into_user_winning_picks_table():
    """
    Function pipelines the process required to insert picks into the winning picks table
    :return: None
    """
    user_games_with_scores_df = make_games_with_scores_df()
    for index, row in user_games_with_scores_df.iterrows():
        make_insert_into_user_winning_picks_table(row["user_id_game_id"], row["user_id"],
                                                  row["game_id"], row["week"],
                                                  row["correct_pick_flag"])
    return user_games_with_scores_df


def make_tab_names(nfl_games_with_scored_df):
    """
    Function makes a list holding NFL Week numbers which have a score against them in the database
    :param nfl_games_with_scored_df: Dataframe with games having a final score
    :return: Pandas Dataframe
    """
    weeks_with_scores = sorted(list(nfl_games_with_scored_df["week"].unique()), reverse=False)
    tab_name_list = ["Week " + week.astype(str) for week in weeks_with_scores]
    return tab_name_list


################################## STREAMLIT ###################################



try:
    # Connect to DB
    con, cursor = connect_to_postgres_database(USER, PASSWORD, DATABASE_NAME, HOST, port="5432")

    # User ID
    user_id = st.session_state["user_id"]

    st.header("Leaderboard!")\

    nfl_games_with_scored_df = make_database_games_with_scores_df()
    user_games_with_scores_df = pipeline_make_insert_into_user_winning_picks_table()
    tab_name_list = make_tab_names(nfl_games_with_scored_df)

    user_weekly_picks_df = user_games_with_scores_df[user_games_with_scores_df["user_id"] ==user_id]
    correct_picks = sum(user_weekly_picks_df["correct_pick_flag"])
    games_played_this_week = len(nfl_games_with_scored_df)
    st.write("You've correctly chosen {} out of the {} games played this week".format(
        correct_picks, games_played_this_week))


    for tab, week in zip(st.tabs(tab_name_list), tab_name_list):
        with tab:
            number_week = int(week.split(" ")[1])
            user_weekly_picks_df = user_games_with_scores_df[(user_games_with_scores_df["week"] ==
                                                       number_week) & (user_games_with_scores_df["user_id"] ==user_id)]
            correct_picks = sum(user_weekly_picks_df["correct_pick_flag"])
            games_played_this_week = len(nfl_games_with_scored_df[nfl_games_with_scored_df["week"] ==
                                                               number_week])
            st.write("You've correctly chosen {} out of the {} games played this week".format(
                correct_picks, games_played_this_week))
except KeyError:
    st.warning("You must login before accessing this page. Please authenticate via the login "
               "menu on the Weekly Picks page.")

