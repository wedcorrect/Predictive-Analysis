import pandas as pd
import psycopg2, joblib
from sqlalchemy import create_engine
from config import settings

homescore_predictor = joblib.load('/app/ml_models/homescore_predictor.pkl')
awayscore_predictor = joblib.load('/app/ml_models/awayscore_predictor.pkl')
outcome_predictor = joblib.load('/app/ml_models/outcome_predictor.pkl')

def first_conversion(element):
    convert = element.split('/')
    if len(convert) == 2:
        if convert[1] == '0':
            convert = float('0')
        else:
            convert = float(convert[0])/float(convert[1])
    else:
        convert = float(convert[0])
    return convert

def second_conversion(element):
    convert = float((element.split('('))[1].replace(')','').replace('%',''))
    return convert

def third_conversion(element):
    convert = float(element.replace('%',''))
    return convert

def stats_loader(dataframe,table_name):
    #PostgreSQL database connection parameters
    connection_params = {
        "host": settings.database_hostname,
        "port": settings.database_port,
        "database": settings.database_name,
        "user": settings.database_user,
        "password": settings.database_password
    }

    #Connect to PostgreSQL
    connection = psycopg2.connect(**connection_params)

    # Create a SQLAlchemy engine
    engine = create_engine(f'postgresql+psycopg2://{settings.database_user}:{settings.database_password}@{settings.database_hostname}/{settings.database_name}')

    dataframe.to_sql(table_name, engine, if_exists='append', index=False)

    connection.commit()
    connection.close()
    print('ML Predictions push to database successfully')
    
def stats_reader():
    #PostgreSQL database connection parameters
    connection_params = {
        "host": settings.database_hostname,
        "port": settings.database_port,
        "database": settings.database_name,
        "user": settings.database_user,
        "password": settings.database_password
    }

    #Connect to PostgreSQL
    connection = psycopg2.connect(**connection_params)
    cursor = connection.cursor()

    query = f"SELECT * FROM pre_game_stats"
    cursor.execute(query)

    result = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    cursor.close()
    connection.close()

    # Create a DataFrame from the fetched data
    from_db = pd.DataFrame(result, columns=column_names)
    return from_db


def ml_predictor(today, yesterday):
    '''This function extracts the pre-game stats from the database and used the trained
    machine learning model to make predictions about the match. It then loads it back into
    the database'''

    match_outcome_dict = {'0':'Home_Win','1':'Home_Lose','2':'Home_Draw'}
    selected_features = ['Away_Big_chances', 'Away_Errors_leading_to_shot', 'Home_freeKicks', 
                        'Away_Clean_sheets', 'Away_Acc._opposition_half', 'Away_Saves_made', 
                        'Away_Free_kick_goals', 'Home_Right_foot_goals', 'Away_Yellow', 'Away_Acc._crosses', 
                        'Away_Left_foot_goals', 'Away_Errors_leading_to_goal', 'Home_Succ._dribbles', 
                        'Away_Penalty_goals', 'Away_blockedScoringAttempt', 'Away_Fouls', 'Away_Clearance_off_line', 
                        'Away_Duels_won', 'Home_Errors_leading_to_goal', 'Away_Succ._dribbles', 'Home_goalKicks', 
                        'Home_Acc._long_balls', 'Home_Clearances_per_game', 'Away_fastBreaks', 'Away_penaltiesCommited', 
                        'Home_Hit_woodwork', 'Home_Big_chances_missed', 'Home_penaltiesCommited', 'Away_Goals_conceded', 
                        'Away_ballRecovery', 'Away_Assists']
    
    match_df = stats_reader()

    match_df['Date'] = pd.to_datetime(match_df['Date'], format="%Y-%m-%d %H:%M:%S") #  format='%d/%m/%Y')
    today_df = match_df[(match_df['Date'].dt.date == today) | (match_df['Date'].dt.date == yesterday)] #Account for when the dataset filter everything due to no matching date
    today_df = today_df.copy(deep=True)

    if today_df.shape[0] > 0:
        #Wrangling scripts
        for col in list(today_df.columns)[6:]:
            try:
                today_df[col] = today_df[col].astype(float)
            except:
                if '/' in list(today_df[col])[0]:
                    today_df[col] = today_df[col].apply(first_conversion)
                elif ('(' in list(today_df[col])[0]) & ('%' in list(today_df[col])[0]):
                    today_df[col] = today_df[col].apply(second_conversion)
                elif ('%' in list(today_df[col])[0]):
                    today_df[col] = today_df[col].apply(third_conversion)

        indep_var = today_df[selected_features].values
        home_score = list(homescore_predictor.predict(indep_var))
        #print(home_score)
        away_score = list(awayscore_predictor.predict(indep_var))
        #print(away_score)
        match_outcome = list(outcome_predictor.predict(indep_var))
        predicted_scores = [f"{round(home_score[i], 0)} - {round(away_score[i], 0)}" for i in range(len(home_score))]
        predicted_outcome = [match_outcome_dict[str(outcome)] for outcome in match_outcome]
        today_df['predicted_scores'] = predicted_scores
        today_df['predicted_outcome'] = predicted_outcome

        finalselected_columns = ['Date','Time','Home_Team','Away_Team','match_links','league','predicted_scores','predicted_outcome']
        final_df = today_df[finalselected_columns]
        stats_loader(final_df, 'match_ml_predictions')


