import pandas as pd
import psycopg2, json
from sqlalchemy import create_engine
from config import settings
from datetime import date, timedelta

rulesexcept_messgs = {}

def teampred_extract():
    '''Extracting the data from the database to load into the dataframe for analysis.'''

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

    #Create the table in the database
    get_query = f"SELECT * FROM match_prediction"
    cursor.execute(get_query)

    rows = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    #Commit and close connection
    cursor.close()
    connection.close()

    #Converting the data extracted to a DataFrame for analysis
    df = pd.DataFrame(rows, columns=column_names)
    df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d %H:%M:%S")

    #yesterday = date.today() + timedelta(days=-1)
    today = date.today()
    tomorrow = date.today() + timedelta(days=1)
    team_df = df[(df['date'].dt.date == today) | (df['date'].dt.date == tomorrow)]
    return team_df

def refpred_extract():
    '''Extracting the data from the database to load into the dataframe for analysis.'''

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

    #Create the table in the database
    get_query = f"SELECT * FROM ref_match_pred"
    cursor.execute(get_query)

    rows = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    #Commit and close connection
    cursor.close()
    connection.close()

    #Converting the data extracted to a DataFrame for analysis
    df = pd.DataFrame(rows, columns=column_names)
    df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d %H:%M:%S")

    #yesterday = date.today() + timedelta(days=-1)
    today = date.today()
    tomorrow = date.today() + timedelta(days=1)
    team_df = df[(df['date'].dt.date == today) | (df['date'].dt.date == tomorrow)]
    return team_df


def rulecheck_loader(dataset):
    '''Extracting the data from the dataframe to load into the database multiple rows at a time'''

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

    #Create the table in the database
    create_query = '''CREATE TABLE IF NOT EXISTS rules_check (
        date VARCHAR,
        hometeam VARCHAR,
        awayteam VARCHAR,
        match_urls VARCHAR,
        home_urls VARCHAR,
        away_urls VARCHAR,
        league VARCHAR,
        home_team_matches JSONB,
        away_team_matches JSONB,
        head2head_matches JSONB,
        home_team_matchespattern JSONB,
        away_team_matchespattern JSONB,
        home_score_patterns JSONB,
        away_score_patterns JSONB,
        h2h_score_patterns JSONB,
        innerdetail_analysis JSONB,
        home_not_lose VARCHAR,
        away_not_lose VARCHAR,
        atleast_one_home VARCHAR,
        atleast_one_away VARCHAR,
        twoormoregoals_total VARCHAR,
        lessthan4goals_total VARCHAR,
        bothteams_score VARCHAR,
        bothteams_notscore VARCHAR,
        cond_check VARCHAR
    );'''
    cursor.execute(create_query)
    connection.commit()

    # Create a SQLAlchemy engine
    engine = create_engine(f'postgresql+psycopg2://{settings.database_user}:{settings.database_password}@{settings.database_hostname}/{settings.database_name}')

    dataset.to_sql('rules_check', engine, if_exists='append', index=False)

    #Commit and close connection
    connection.commit()
    cursor.close()
    connection.close()


def rules_check():
    '''This function check for the rules and filters only the predictions that pass the rules'''
    for i in range(2):
        try:
            prediction = teampred_extract()
            break
        except Exception as e:
            rulesexcept_messgs[f"(Data Extraction)"] = f"{type(e).__name__}: {e}"
            prediction = pd.DataFrame([], columns=['date', 'hometeam', 'awayteam', 'match_urls', 'home_urls', 'away_urls',
                                                   'league', 'home_team_matches', 'away_team_matches', 'head2head_matches',
                                                   'home_team_matchespattern', 'away_team_matchespattern',
                                                   'home_score_patterns', 'away_score_patterns', 'h2h_score_patterns',
                                                   'innerdetail_analysis'])

    if prediction.shape[0] > 0:
        print(prediction.shape[0])

#         for i in range(3):
#             try:
#                 ref_predictions = refpred_extract()
#                 break
#             except:
#                 ref_predictions = pd.DataFrame([], columns=['date', 'time', 'hometeam', 'awayteam', 'result', 'matchlink', 'league',
#                                                             'refereelink', 'referee_matchistlink', 'referee_matchhistdetails',
#                                                             'ref_patterns'])

#         print(ref_predictions.shape[0])
#         corr_refpred = [] #Correspondng Referee Prediction for the same Match set up.

#         if (ref_predictions.shape[0] > 0) & (prediction.shape[0] > 0):
#             for i in range(prediction.shape[0]):
#                 for j in range(ref_predictions.shape[0]):
#                     if (list(prediction['hometeam'])[i] in list(ref_predictions['hometeam'])[j]) & (list(prediction['awayteam'])[i] in list(ref_predictions['awayteam'])[j]) & (list(prediction['league'])[i] == list(ref_predictions['league'])[j]):
#                         corr_refpred.append(list(ref_predictions['ref_patterns'])[j])
#                         break
#                     elif j == (ref_predictions.shape[0]-1):
#                         corr_refpred.append({})
#                         break

#        print(prediction.shape[0]), len(corr_refpred))

        #Rules Check
        col_of_prediction = ['home_score_patterns', 'away_score_patterns', 'h2h_score_patterns']

#         if len(corr_refpred) > 0:
#             prediction['ref_predictions'] = corr_refpred
#             col_of_prediction = ['home_score_patterns', 'away_score_patterns', 'h2h_score_patterns', 'ref_predictions']

        #Rules Check dictionary to be printed along with other predictions
        rules_list = {'home_not_lose':[],'away_not_lose':[],'atleast_one_home':[],'atleast_one_away':[],
                      'twoormoregoals_total':[],'lessthan4goals_total':[],'bothteams_score':[],
                      'bothteams_notscore':[]
                     }

        #Variables for the check
        home_not_lose_count, home_not_lose_count1 = 0, 0
        away_not_lose_count, away_not_lose_count1  = 0, 0
        atleast_one_home_count, atleast_one_home_count1  = 0, 0
        atleast_one_away_count, atleast_one_away_count1  = 0, 0
        twoormoregoals_total_count, twoormoregoals_total_count1  = 0, 0
        lessthan4goals_total_count, lessthan4goals_total_count1  = 0, 0
        bothteams_score_count, bothteams_score_count1  = 0, 0
        bothteams_notscore_count, bothteams_notscore_count1  = 0, 0

        threshold = 0.85

        #Function for checking all the rule and updating the dictionary to be printed.
        for i in range(prediction.shape[0]):

            for column in col_of_prediction:
                if 'NoneType' not in str(type(list(prediction[column])[0])):
                    for key in (list(prediction[column])[i]).keys():
                        score = str((list(prediction[column])[i])[key][-5:])
                        score = score.replace(' ','')
                        score = score.split('-')

                        home_not_lose_count += 1
                        away_not_lose_count  += 1
                        atleast_one_home_count  += 1
                        atleast_one_away_count  += 1
                        twoormoregoals_total_count  += 1
                        lessthan4goals_total_count  += 1
                        bothteams_score_count  += 1
                        bothteams_notscore_count  += 1

                        if (float(score[0]) < float(score [1])):
                            home_not_lose_count1 +=1
                        if (float(score[1]) < float(score [0])):
                            away_not_lose_count1  += 1
                        if (float(score[0]) < 1):
                            atleast_one_home_count1  += 1
                        if (float(score[1]) < 1):
                            atleast_one_away_count1  += 1
                        if (float(score[0]) + float(score [1])) < 2:
                            twoormoregoals_total_count1  += 1
                        if (float(score[0]) + float(score [1])) >= 4:
                            lessthan4goals_total_count1  += 1
                        if (float(score[0]) < 1) | (float(score[1]) < 1):
                            bothteams_score_count1  += 1
                        if (float(score[0]) > 1) | (float(score[1]) > 1):
                            bothteams_notscore_count1  += 1

            #Checks for which of the matches exceed the threshold
            if (1 - (home_not_lose_count1/home_not_lose_count)) >= threshold:
                rules_list['home_not_lose'].append('True')
            else:
                rules_list['home_not_lose'].append('False')
            if (1 - (away_not_lose_count1/away_not_lose_count)) >= threshold:
                rules_list['away_not_lose'].append('True')
            else:
                rules_list['away_not_lose'].append('False')
            if (1 - (atleast_one_home_count1/atleast_one_home_count)) >= threshold:
                rules_list['atleast_one_home'].append('True')
            else:
                rules_list['atleast_one_home'].append('False')
            if (1 - (atleast_one_away_count1/atleast_one_away_count)) >= threshold:
                rules_list['atleast_one_away'].append('True')
            else:
                rules_list['atleast_one_away'].append('False')
            if (1 - (twoormoregoals_total_count1/twoormoregoals_total_count)) >= threshold:
                rules_list['twoormoregoals_total'].append('True')
            else:
                rules_list['twoormoregoals_total'].append('False')
            if (1 - (lessthan4goals_total_count1/lessthan4goals_total_count)) >= threshold:
                rules_list['lessthan4goals_total'].append('True')
            else:
                rules_list['lessthan4goals_total'].append('False')
            if (1 - (bothteams_score_count1/bothteams_score_count)) >= threshold:
                rules_list['bothteams_score'].append('True')
            else:
                rules_list['bothteams_score'].append('False')
            if (1 - (bothteams_notscore_count1/bothteams_notscore_count)) >= threshold:
                rules_list['bothteams_notscore'].append('True')
            else:
                rules_list['bothteams_notscore'].append('False')

        for key in list(rules_list.keys()):
            prediction[key] = rules_list[key]

        #Converts the columns with dictionaries to json format for loading
        col_list = ['home_team_matches','away_team_matches','head2head_matches','home_team_matchespattern','away_team_matchespattern','home_score_patterns','away_score_patterns','h2h_score_patterns','innerdetail_analysis']

#         if len(corr_refpred) > 0:
#             col_list = ['home_team_matches','away_team_matches','head2head_matches','home_team_matchespattern','away_team_matchespattern','home_score_patterns','away_score_patterns','h2h_score_patterns','innerdetail_analysis','ref_predictions']

        for column in col_list:
            prediction[column] = prediction[column].apply(json.dumps)

        cond_check = []
        temp_df = prediction[['home_not_lose','away_not_lose','atleast_one_home','atleast_one_away','twoormoregoals_total','lessthan4goals_total','bothteams_score','bothteams_notscore']]
        for i in range(temp_df.shape[0]):
            check = list(temp_df.iloc[i,:])
            if 'True' in check:
                cond_check.append('True')
            else:
                cond_check.append('False')

        prediction['cond_check'] = cond_check
        
        #Load the checked rules into the database
        for i in range(2):
            try:
                rulecheck_loader(prediction)
                break
            except Exception as e:
                rulesexcept_messgs[f"(Data Extraction)"] = f"{type(e).__name__}: {e}"
