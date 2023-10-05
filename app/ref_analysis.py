import pandas as pd
import psycopg2, json
from sqlalchemy import create_engine
from config import settings

refexcept_messgs = {}

def refdata_extract():
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
    create_query = '''SELECT * FROM ref_historic_match'''
    cursor.execute(create_query)

    # Fetch all rows
    rows = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]

    #Converting the data extracted to a DataFrame for analysis
    df_ref = pd.DataFrame(rows, columns=column_names)
    df_ref

    #Commit and close connection
    connection.commit()
    cursor.close()
    connection.close()

    return df_ref


def ref_hist_analysis(team_matches, skip=False):
    '''This function analyses the table without any filters and simple checks for pattern for a given
    role (home or away). The historic matches of the team to play the given role in the upcoming match is
    analysed for the given role'''
    
    #team_matches = json.loads(team_matches)
    team_df = pd.DataFrame(team_matches)
    team_df['home_club_goal'] = team_df['home_club_goal'].astype(int)
    team_df['away_club_goal'] = team_df['away_club_goal'].astype(int)
    
    end_string = 'Score prediction based on ref history: '
    
    if skip == True:
        for i in range(team_df.shape[0]):
            if (i%2) == 0:
                team_df.drop(i, inplace=True)
        end_string = 'Score prediction based on ref history after skipping rows: '
    
    third_recentscore = [list(team_df['home_club_goal'])[2], list(team_df['away_club_goal'])[2]]
    
    def check(dataframe, column):
        varb = list(dataframe[column])[:3]
        if len(varb) <= 1: #Checks if there's enough data to check for a pattern
            return ['-']
        else:
            #Checks for all the patterns for a particular role regardless of which team played
            if (varb[0] == varb[1]):
                return [varb[0]]
            elif ((max(varb[:2]) - min(varb[:2])) == 1):
                if 0 in varb[:2]:
                    return [max(varb[:2])+1]
                else:
                    return [max(varb[:2])+1, min(varb[:2])-1]
            elif ((max(varb[:2]) - min(varb[:2])) == 2):
                return [max(varb[:2])-1]
            else:
                return ['-']
    
    scores = []    
    scores.append(check(team_df, 'home_club_goal'))
    scores.append(check(team_df, 'away_club_goal'))
    
    combined_scores = []
    final_output = []
    if ['-'] not in scores:
        #Gets all the possible predictions from the observed patterns if any
        for score1 in scores[0]:
            for score2 in scores[1]:
                combined_scores.append([score1, score2])
                
        #Checks and removes the third most recent match outcome if it already exists in the list of possible predictions
        if third_recentscore in combined_scores:
            pass
        else:
            for score in combined_scores:
                final_string = end_string + '{} - {}'.format(score[0], score[1])
                final_output.append(final_string)
    else:
        pass
    return final_output


def ref_total_analysis(dataset):
    '''This function takes in an entire row of the dataset pulled from the database and 
    extracts all the observed patterns.'''
    dict_of_patterns = {'ref_patterns':[]}
    
    for i in range(dataset.shape[0]):
        ref_row = list(dataset.iloc[i,:])
        
        if ref_row[9] != {}:
            ref_hist = pd.DataFrame(ref_row[9])
            #print(list(ref_hist.columns))
            ref_hist['Date'] = pd.to_datetime(ref_hist['Date'], format='%d/%m/%Y')
            ref_hist[['home_club_goal', 'away_club_goal']] = ref_hist['Score'].str.split(':', n=1, expand=True)
            ref_hist = ref_hist[~ref_hist['away_club_goal'].str.contains('pso')]
            ref_hist = ref_hist[~ref_hist['home_club_goal'].str.contains('pso')]
            ref_hist = ref_hist[~ref_hist['away_club_goal'].str.contains('aet')]
            ref_hist = ref_hist[~ref_hist['home_club_goal'].str.contains('aet')]
            ref_hist = ref_hist[~ref_hist['away_club_goal'].str.contains('dec')]
            ref_hist = ref_hist[~ref_hist['home_club_goal'].str.contains('dec')]
            ref_hist.sort_values(by='Date', ascending=False, inplace=True)
            ref_hist.reset_index(inplace=True)
            
            dict_of_pattern = {}
            list_of_pattern = []
            try:
                patterns = ref_hist_analysis(ref_hist)
                list_of_pattern = list_of_pattern + [pattern for pattern in patterns]
            except Exception as e:
                refexcept_messgs[f"ref_hist_analysis: {str(i)}"] = f"{type(e).__name__}: {e}" #Catches and Records Error
                list_of_pattern = list_of_pattern + []
            try:
                patterns = ref_hist_analysis(ref_hist, skip=True)
                list_of_pattern = list_of_pattern + [pattern for pattern in patterns]
            except Exception as e:
                refexcept_messgs[f"ref_hist_analysis (skip): {str(i)}"] = f"{type(e).__name__}: {e}" #Catches and Records Error
                list_of_pattern = list_of_pattern + []
            
            for i in range(len(list_of_pattern)):
                dict_of_pattern[str(i)] = list_of_pattern[i]

            dict_of_patterns['ref_patterns'].append(json.dumps(dict_of_pattern))
        else:
            dict_of_patterns['ref_patterns'].append(json.dumps({}))
    return dict_of_patterns


def refdata_loader(ref_modified_dataset):
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
    create_query = '''CREATE TABLE IF NOT EXISTS ref_match_pred (
        date VARCHAR,
        time VARCHAR,
        hometeam VARCHAR,
        awayteam VARCHAR,
        result VARCHAR,
        matchlink VARCHAR,
        league VARCHAR,
        refereelink VARCHAR,
        referee_matchistlink JSONB,
        referee_matchhistdetails JSONB,
        ref_patterns JSONB
        
    );'''
    cursor.execute(create_query)
    connection.commit()

    # Create a SQLAlchemy engine
    engine = create_engine(f'postgresql+psycopg2://{settings.database_user}:{settings.database_password}@{settings.database_hostname}/{settings.database_name}')

    ref_modified_dataset.to_sql('ref_match_pred', engine, if_exists='append', index=False)

    #Commit and close connection
    connection.commit()
    cursor.close()
    connection.close()


def ref_analysis_flow(today, tomorrow):
    #Team Match Analysis
    for i in range(2): #Tries twice to extract the data from the database
        try:
            ref_df = refdata_extract() #If try is successful, breaks the loop
            break
        except Exception as e:
            refexcept_messgs[f"(Database Extraction): {str(i)}"] = f"{type(e).__name__}: {e}" #Catches and Records Error
    
    try:
        #Converts the date column for filtering
        ref_df['date'] = pd.to_datetime(ref_df['date'], format="%Y-%m-%d %H:%M:%S") #  format='%d/%m/%Y')
        today_df = ref_df[(ref_df['date'].dt.date == today) | (ref_df['date'].dt.date == tomorrow)] #Account for when the dataset filter everything due to no matching date
        today_df = today_df.copy(deep=True)
    except Exception as e:
        refexcept_messgs[f"(Data Wrangling)"] = f"{type(e).__name__}: {e}" #Catches and Records Error
    
    try:
        #Analyses the dataset and generates the predictions
        ref_additional_columns = ref_total_analysis(today_df)
    except Exception as e:
        refexcept_messgs[f"(Data Analysis)"] = f"{type(e).__name__}: {e}" #Catches and Records Error
    
    try:
        ref_modified_dataset = today_df.copy(deep=True)
        #Adds the prediction to the original dataset
        for key in ref_additional_columns.keys():
            ref_modified_dataset[key] = ref_additional_columns[key]
    except Exception as e:
        refexcept_messgs[f"(Prediction Addition)"] = f"{type(e).__name__}: {e}" #Catches and Records Error
    
    try:
        #Converts the columns with dictionaries to json format for loading
        col_list = ['referee_matchistlink','referee_matchhistdetails']
        for column in col_list:
            ref_modified_dataset[column] = ref_modified_dataset[column].apply(json.dumps)
        ref_modified_dataset
    except Exception as e:
        refexcept_messgs[f"(Datatype Transformation)"] = f"{type(e).__name__}: {e}" #Catches and Records Error
    
    try:
        refdata_loader(ref_modified_dataset)
    except Exception as e:
        refexcept_messgs[f"(Database Loading)"] = f"{type(e).__name__}: {e}" #Catches and Records Error