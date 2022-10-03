import streamlit as st
import pandas as pd
import psycopg2
from psycopg2 import Error
from sqlalchemy import create_engine
import plotly.graph_objects as go


# Vars
USER = st.secrets["USER"]
PASSWORD = st.secrets["PASSWORD"]
DATABASE_NAME = st.secrets["DATABASE_NAME"]
HOST = st.secrets["HOST"]
PORT = st.secrets["PORT"]
CORRECT_COLOR = "#41b45c"
WRONG_COLOR = "#F05454"
NEUTRAL_COLOR = "#EFEFEF"


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


@st.cache(allow_output_mutation=True, show_spinner=False)
def make_database_games_with_scores_df():
    """
    Function queries the nfl_game_scores_2022 table and returns a Pandas DataFrame
    :return: Dataframe
    """
    engine = create_engine("postgresql+psycopg2://" + USER + ":" + PASSWORD + "@" + HOST + "/" +
                           DATABASE_NAME)
    query = """
         SELECT *
         FROM nfl_game_scores_2022
         ;
         """
    database_games_with_scores_df = pd.read_sql_query(query, con=engine)
    return database_games_with_scores_df


@st.cache(allow_output_mutation=True, show_spinner=False)
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


@st.cache(allow_output_mutation=True, show_spinner=False)
def make_user_picks_with_win_df(user_id):
    """
    Function checks the dashboard_user table and returns the user_id by referencing a username
    :param username: username
    :return: user_id
    """
    engine = create_engine("postgresql+psycopg2://" + USER + ":" + PASSWORD + "@" + HOST + "/" +
                           DATABASE_NAME)
    query = """WITH user_picks AS (
                        SELECT game_id, winning_pick
                        FROM user_weekly_picks
                        WHERE user_id = %(user_id)s
                        ),
                        nfl_game_scores AS (
                            SELECT game_id, away_team, home_team,
                                   CASE
                        WHEN home_score > away_score THEN home_team
                        WHEN home_score < away_score THEN away_team
                        ELSE NULL
                        END AS nfl_winning_team
                        FROM nfl_game_scores_2022
                        ),
                        left_join AS (
                            SELECT usr.game_id, nfl.away_team, nfl.home_team, nfl.nfl_winning_team, usr.winning_pick
                        FROM user_picks usr
                        LEFT JOIN nfl_game_scores nfl
                        ON usr.game_id = nfl.game_id
                        ),
                        case_statement AS (
                            SELECT game_id, away_team, home_team, winning_pick,
                                   CASE
                        WHEN winning_pick = nfl_winning_team THEN 1
                        WHEN winning_pick != nfl_winning_team THEN 0
                        ELSE 0
                        END AS correct_or_not
                        FROM left_join
                        )
                        SELECT * FROM  case_statement;"""
    user_picks_with_win_df = pd.read_sql_query(query, con=engine, params={"user_id": user_id})
    return user_picks_with_win_df


def make_matchup_score_dicts(row, away_score_dict, home_score_dict):
    """
    Function adds matchup information to two different dictionaries
    :param row: Row from Pandas.DataFrame.iterrows()
    :param away_score_dict: Dict holding matchup as key and away score as value - empty
    :param home_score_dict: Dict holding matchup as key and home score as value - empty
    :return: None
    """
    dict_key = row["away_team"] + " @ " + row["home_team"]
    away_dict_value = row["away_score"]
    away_score_dict[dict_key] = away_dict_value
    home_dict_value = row["home_score"]
    home_score_dict[dict_key] = home_dict_value
    return None


def make_team_color_logic(dataframe, away_team_color_list, home_team_color_list):
    """
    Function appends a color, depending on the correctness of a pick, to a list
    :param dataframe: Dataframe holding single row of user pick data
    :param away_team_color_list: List holding color or away teams - empty
    :param home_team_color_list: List holding color or home teams - empty
    :return: None
    """
    if dataframe["correct_or_not"].iloc[0] == 1:
        if dataframe["winning_pick"].iloc[0] == dataframe["home_team"].iloc[0]:
            home_team_color_list.append(CORRECT_COLOR)
            away_team_color_list.append(NEUTRAL_COLOR)
        elif dataframe["winning_pick"].iloc[0] == dataframe["away_team"].iloc[0]:
            home_team_color_list.append(NEUTRAL_COLOR)
            away_team_color_list.append(CORRECT_COLOR)
    if dataframe["correct_or_not"].iloc[0] == 0:
        if dataframe["winning_pick"].iloc[0] == dataframe["home_team"].iloc[0]:
            home_team_color_list.append(WRONG_COLOR)
            away_team_color_list.append(NEUTRAL_COLOR)
        elif dataframe["winning_pick"].iloc[0] == dataframe["away_team"].iloc[0]:
            home_team_color_list.append(NEUTRAL_COLOR)
            away_team_color_list.append(WRONG_COLOR)
    return None


def pipeline_make_matchup_dicts_team_color_logic(nfl_games_with_scores_df, week, user_picks_with_win_df):
    """
    Function pipelines the process needed to create dictioanries holding scores and lists holding colors
    :param nfl_week_game_score_df: Dataframe with nfl games and scores
    :param week: NFL week number
    :param user_picks_with_win_df: Dataframe with a users matchup pick and correct flag
    :return: away_score_dict, home_score_dict, away_team_color_list, home_team_color_list
    """
    away_score_dict = dict()
    home_score_dict = dict()
    away_team_color_list = list()
    home_team_color_list = list()
    nfl_week_game_score_df = nfl_games_with_scores_df[nfl_games_with_scores_df["week"] == week]
    for index, row in nfl_week_game_score_df.iterrows():
        make_matchup_score_dicts(row, away_score_dict, home_score_dict)
        if row["game_id"] in list(user_picks_with_win_df["game_id"]):
            temp_df = user_picks_with_win_df[user_picks_with_win_df["game_id"] == row["game_id"]]
            make_team_color_logic(temp_df, away_team_color_list, home_team_color_list)
    return away_score_dict, home_score_dict, away_team_color_list, home_team_color_list


def make_plot_matchup_scores(away_score_dict, home_score_dict, away_team_color_list, home_team_color_list):
    """
    Function plots a weeks matchup on the xaxis and team scores on the yaxis. Color is used to show correct/incorrect and not picked
    :param away_score_dict: Dict holding matchup as key and away score as value
    :param home_score_dict: Dict holding matchup as key and home score as value
    :param away_team_color_list: List holding color or away teams
    :param home_team_color_list: List holding color or home teams
    :return: Plotly bar chart object
    """
    fig = go.Figure(data=[
        go.Bar(x=list(home_score_dict.keys()), y=list(away_score_dict.values()), marker_color=away_team_color_list),
        go.Bar(x=list(home_score_dict.keys()), y=list(home_score_dict.values()), marker_color=home_team_color_list)
    ])
    fig.update_layout(barmode='group', template="plotly_dark", xaxis=dict(showgrid=False), yaxis=dict(showgrid=False), showlegend=False)
    return fig


def make_pipeline_plot_matchup_scores(nfl_games_with_scores_df, week, user_picks_with_win_df):
    """
    Function pipelines the process needed to create a plot showing a weeks matchup on the xaxis and team scores on the yaxis
    :param nfl_week_game_score_df: Dataframe with nfl games and scores
    :param week: NFL week number
    :param user_picks_with_win_df: Dataframe with a users matchup pick and correct flag
    :return: Plotly bar chart object
    """
    away_score_dict, home_score_dict, away_team_color_list, home_team_color_list = pipeline_make_matchup_dicts_team_color_logic(nfl_games_with_scores_df, week, user_picks_with_win_df)
    fig = make_plot_matchup_scores(away_score_dict, home_score_dict, away_team_color_list,
                              home_team_color_list)
    return fig


################################## STREAMLIT ###################################


# Connect to DB
con, cursor = connect_to_postgres_database(USER, PASSWORD, DATABASE_NAME, HOST, port="5432")

try:
    # User ID
    user_id = st.session_state["user_id"]

    st.header("My !")

    with st.spinner("Getting your picks..."):
        nfl_games_with_scored_df = make_database_games_with_scores_df()
        user_games_with_scores_df = make_games_with_scores_df()
        user_picks_with_win_df = make_user_picks_with_win_df(user_id)
    tab_name_list = make_tab_names(nfl_games_with_scored_df)

    user_weekly_picks_df = user_games_with_scores_df[user_games_with_scores_df["user_id"] ==user_id]
    correct_picks = sum(user_weekly_picks_df["correct_pick_flag"])
    games_played_this_week = len(nfl_games_with_scored_df)
    st.write("You've correctly chosen {} out of the {} games played this season".format(
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


            fig = make_pipeline_plot_matchup_scores(nfl_games_with_scored_df, number_week,
                                               user_picks_with_win_df)
            st.plotly_chart(fig, use_container_width=True)

except KeyError:
    st.warning("You must login before accessing this page. Please authenticate via the login "
               "menu on the Weekly Picks page.")

