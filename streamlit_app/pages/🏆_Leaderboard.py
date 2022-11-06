import streamlit as st
import pandas as pd
import psycopg2
from psycopg2 import Error
from sqlalchemy import create_engine
import plotly.graph_objects as go
from plotly.subplots import make_subplots


# Vars
USER = st.secrets["USER"]
PASSWORD = st.secrets["PASSWORD"]
DATABASE_NAME = st.secrets["DATABASE_NAME"]
HOST = st.secrets["HOST"]
PORT = st.secrets["PORT"]



def make_leaderboard_df():
    """
    Function queries the database to return a leaderboard showing username, games played
    :return: Dataframe
    """
    engine = create_engine("postgresql+psycopg2://" + USER + ":" + PASSWORD + "@" + HOST + "/" +
                           DATABASE_NAME)
    query = """
            SELECT usr.username,
                   SUM(pck.correct_pick_flag) AS Correct_Picks,
                   100 * ROUND(CAST(SUM(pck.correct_pick_flag) AS numeric) / CAST(COUNT(
                   pck.game_id) AS numeric), 3) AS pct_correct,
                   COUNT(DISTINCT pck.week) AS weekls_played
            FROM user_winning_picks pck
            LEFT JOIN users usr
                ON pck.user_id = usr.user_id
            GROUP BY 1
            ORDER BY 2 DESC
         ;
         """
    leaderboard_df = pd.read_sql_query(query, con=engine)
    return leaderboard_df


def make_pct_correct_by_week_df():
    """
    Function queries the database to a DataFrame showing the games correct (as a percentage) that each user has had correct
    :return: Dataframe
    """
    engine = create_engine("postgresql+psycopg2://" + USER + ":" + PASSWORD + "@" + HOST + "/" +
                           DATABASE_NAME)
    query = """
            WITH nfl_games_per_week AS (
                SELECT
                    week,
                    COUNT(game_id) AS count_of_games
                FROM
                     nfl_games_2022
                GROUP BY 1
            ),
                won_by_week AS(
                SELECT
                    usr.username,
                    pck.week,
                    CAST(SUM(pck.correct_pick_flag) AS numeric) AS correct_picks
                FROM user_winning_picks pck
                LEFT JOIN users usr
                    ON pck.user_id = usr.user_id
                GROUP BY 1, 2
            ),
                pct_won_by_week AS (
                SELECT
                    won.username,
                    won.week,
                    won.correct_picks,
                    ROUND((won.correct_picks / nfl.count_of_games), 3) AS pct_correct
                FROM
                    won_by_week won
                LEFT JOIN
                    nfl_games_per_week nfl
                    ON won.week = nfl.week
            )
            SELECT * FROM pct_won_by_week
            ORDER BY 2, 1
         ;
         """
    pct_correct_by_week_df = pd.read_sql_query(query, con=engine)
    return pct_correct_by_week_df


def make_pct_correct_by_week_plot(pct_correct_by_week_df):
    """
    Function plots the percentage of games which have been correct per user by week
    :param make_pct_correct_by_week_df:
    :return: Plotly object
    """
    fig = go.Figure()
    for username in pct_correct_by_week_df["username"].unique():
        temp_df = pct_correct_by_week_df[pct_correct_by_week_df["username"] == username]
        fig.add_trace(go.Scatter(x=list(temp_df["week"]), y=list(temp_df["pct_correct"]), name=username, line_shape='linear'))
    fig.update_layout(template="plotly_dark", xaxis_title="Week")
    fig.update_yaxes(tickformat="%")
    return fig


def make_pipeline_pct_correct_by_week():
    """
    Function pipelines the process required to plot the percentage of games which have been correct per user by week
    :return: Plotly object
    """
    pct_correct_by_week_df = make_pct_correct_by_week_df()
    fig = make_pct_correct_by_week_plot(pct_correct_by_week_df)
    return fig


######################################### RUN #######################################

try:
    c1, c2, c3 = st.columns(3)
    with c2:
        # User ID
        user_id = st.session_state["user_id"]

        st.header("Leaderboard 🥇")

        st.dataframe(make_leaderboard_df().style.format({"pct_correct" : '{:.1f}%'}))

        st.plotly_chart(make_pipeline_pct_correct_by_week(), use_container_width=True)

except KeyError:
    st.warning("You must login before accessing this page. Please authenticate via the login "
               "menu on the Weekly Picks page.")
