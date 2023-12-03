import pandas as pd
import psycopg2, json
from sqlalchemy import create_engine
from collections import Counter
from config import settings
from var import leagues_abbrv

except_messgs = {}
pred_count = {}

def teamdata_extract():
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
    create_query = '''SELECT * FROM historic_match'''
    cursor.execute(create_query)

    # Fetch all rows
    rows = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]

    #Converting the data extracted to a DataFrame for analysis
    df = pd.DataFrame(rows, columns=column_names)
    df

    #Commit and close connection
    connection.commit()
    cursor.close()
    connection.close()

    return df


def indiv_teamrole_analysis(team_matches, team_name, role, skip=False, by_league=None):
    '''This function filters the table by only matches played by the team under analysis where the said
    team played the role they will play in their upcoming match'''
    
    #team_matches = json.loads(team_matches)
    team_df = pd.DataFrame(team_matches)
    team_df['home_club_goal'] = team_df['home_club_goal'].astype(int)
    team_df['away_club_goal'] = team_df['away_club_goal'].astype(int)
        
    #Creating the first part of the final string to describe the prediction depending on which set of historical matches (home, away of head-to-head)
    list_of_clubs = list(team_df['home_club']) + list(team_df['away_club'])
    num_of_club = len(set(list_of_clubs))
    if num_of_club > 2:
        end_string = 'Score prediction for {} team using {} historic match scores, for only matches where {} team played {} role: '.format(role, role, role, role)
    else:
        end_string = 'Score prediction for both teams using head-to-head historic match scores, for only matches where they played the same role as their upcoming game: '
    
    if by_league != None:
        team_df = team_df[team_df['league'] == by_league[1]]
        
        if num_of_club > 2:
            end_string = 'Score prediction for {} team using {} historic match scores filtered by {}, for only matches where {} team played {} role: '.format(role, role, by_league[0], role, role)
        else:
            end_string = 'Score prediction for both team using head-to-head historic match scores filtered by {}, for only matches where they played the same role as their upcoming game: '.format(by_league[0])
    
    if skip == True:
        for i in range(team_df.shape[0]):
            if (i%2) == 0:
                team_df.drop(i, inplace=True)
                
        if num_of_club > 2:
            end_string = 'Score prediction for {} team using {} historic match scores after skipping rows, for only matches where {} team played {} role: '.format(role, role, role, role)
        else:
            end_string = 'Score prediction for both team using head-to-head historic match scores after skipping rows, for only matches where they played the same role as their upcoming game: '
    
    if role == 'home':
        if len(list(team_df[team_df['home_club']==team_name])) > 2:
            third_recentscore = [list(team_df[team_df['home_club']==team_name]['home_club_goal'])[2], list(team_df[team_df['away_club']!=team_name]['away_club_goal'])[2]]
        else:
            third_recentscore = []
    else:
        if len(list(team_df[team_df['away_club']==team_name])) > 2:
            third_recentscore = [list(team_df[team_df['home_club']!=team_name]['home_club_goal'])[2], list(team_df[team_df['away_club']==team_name]['away_club_goal'])[2]]
        else:
            third_recentscore = []
            
    def check(team_df, column1, column2):
        varb = list((team_df[team_df[column1] == team_name])[column2])[:3] #Filter by a given team in a given role
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
    if role == 'home':
        scores.append(check(team_df, 'home_club', 'home_club_goal'))
        scores.append(check(team_df, 'home_club', 'away_club_goal'))
    else:
        scores.append(check(team_df, 'away_club', 'home_club_goal'))
        scores.append(check(team_df, 'away_club', 'away_club_goal'))
        
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


def indiv_role_analysis(team_matches, role, skip=False, by_league=None):
    '''This function analyses the table without any filters and simple checks for pattern for a given
    role (home or away). The historic matches of the team to play the given role in the upcoming match is
    analysed for the given role'''
    
    #team_matches = json.loads(team_matches)
    team_df = pd.DataFrame(team_matches)
    team_df['home_club_goal'] = team_df['home_club_goal'].astype(int)
    team_df['away_club_goal'] = team_df['away_club_goal'].astype(int)
    
    #Creating the first part of the final string to describe the prediction depending on which set of historical matches (home, away of head-to-head)
    list_of_clubs = list(team_df['home_club']) + list(team_df['away_club'])
    num_of_club = len(set(list_of_clubs))
    if num_of_club > 2:
        end_string = 'Score prediction for {} role using {} historic match scores, regardless of role played by {} team: '.format(role, role, role)
    else:
        end_string = 'Score prediction for both roles using head-to-head historic match scores, regardless of role played by both teams: '
    
    if by_league != None:
        team_df = team_df[team_df['league'] == by_league[1]]
        
        if num_of_club > 2:
            end_string = 'Score prediction for {} role using {} historic match scores filtered by {}, regardless of role played by {} team: '.format(role, role, by_league[0], role)
        else:
            end_string = 'Score prediction for both roles using head-to-head historic match scores filtered by {}, regardless of role played by both teams: '.format(by_league[0])
    
    if skip == True:
        for i in range(team_df.shape[0]):
            if (i%2) == 0:
                team_df.drop(i, inplace=True)
                
        if num_of_club > 2:
            end_string = 'Score prediction for {} role using {} historic match scores after skipping rows, regardless of role played by {} team: '.format(role, role, role)
        else:
            end_string = 'Score prediction for both roles using head-to-head historic match scores after skipping rows, regardless of role played by both teams: '

    
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


def indiv_team_analysis(team_matches, team_name, role, skip=False, by_league=None):
    '''This function analyses a given team regardless of which role they played in their historic matches.
    It simply checks how they performed against any opponent and if there are any underlying patterns'''
    
    #team_matches = json.loads(team_matches)
    team_df = pd.DataFrame(team_matches)
    team_df['home_club_goal'] = team_df['home_club_goal'].astype(int)
    team_df['away_club_goal'] = team_df['away_club_goal'].astype(int)
    
    #Creating the first part of the final string to describe the prediction depending on which set of historical matches (home, away of head-to-head)
    list_of_clubs = list(team_df['home_club']) + list(team_df['away_club'])
    num_of_club = len(set(list_of_clubs))
    if num_of_club > 2:
        end_string = 'Score prediction for {} team using {} historic match scores, regardless of role played by {} team: '.format(role, role, role)
    else:
        end_string = 'Score prediction for both teams using head-to-head historic match scores, regardless of role played by both teams: '
    
    if by_league != None:
        team_df = team_df[team_df['league'] == by_league[1]]
        
        if num_of_club > 2:
            end_string = 'Score prediction for {} team using {} historic match scores filtered by {}, regardless of role played by {} team: '.format(role, role, by_league[0], role)
        else:
            end_string = 'Score prediction for both teams using head-to-head historic match scores filtered by {}, regardless of role played by both teams: '.format(by_league[0])
    
    if skip == True:
        for i in range(team_df.shape[0]):
            if (i%2) == 0:
                team_df.drop(i, inplace=True)
        
        if num_of_club > 2:
            end_string = 'Score prediction for {} team using {} historic match scores after skipping rows, regardless of role played by {} team: '.format(role, role, role)
        else:
            end_string = 'Score prediction for both teams using head-to-head historic match scores after skipping rows, regardless of role played by both teams: '
        
    
    rearranged = []
    #Rearrangement of the historic table to put the team under analysis in one side and all opponent on the other side.
    for i in range(team_df.shape[0]):
        if list(team_df.iloc[i,:])[2] == team_name:
            rearranged.append(list(team_df.iloc[i,:]))
        else:
            temp_list = list(team_df.iloc[i,:])
            temp_var = temp_list[2]
            temp_list[2] = temp_list[3]
            temp_list[3] = temp_var
            temp_var = temp_list[4]
            temp_list[4] = temp_list[5]
            temp_list[5] = temp_var
            rearranged.append(temp_list)
    new_team_df = pd.DataFrame(rearranged, columns=['date','league','team','opponent','team_score','opponent_score'])
    
    if role == 'home':
        third_recentscore = [list(new_team_df['team_score'])[2], list(new_team_df['opponent_score'])[2]]
    else:
        third_recentscore = [list(new_team_df['opponent_score'])[2], list(new_team_df['team_score'])[2]]
    
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
    scores.append(check(new_team_df, 'team_score'))
    scores.append(check(new_team_df, 'opponent_score'))
    
    combined_scores = []
    final_output = []
    if ['-'] not in scores:
        if role == 'home':
            for score1 in scores[0]:
                for score2 in scores[1]:
                    combined_scores.append([score1, score2])
        else:
            for score1 in scores[1]:
                for score2 in scores[0]:
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


def windrawloss_analysis(team_matches, team_name, role, skip=False):
    '''This function analyses the scores of the historic match regardless of team or role and regardless
    of orientation of the scores. Hence if simply gets the match scores and reorders them in descending order
    and check if there's a pattern (repetition). It also check for match outcome pattern (win, loss, draw),
    and if the team under analysis played different roles during this observed pattern'''
    
    #team_matches = json.loads(team_matches)
    team_df = pd.DataFrame(team_matches)
    team_df['home_club_goal'] = team_df['home_club_goal'].astype(int)
    team_df['away_club_goal'] = team_df['away_club_goal'].astype(int)
    
    #Creating the first part of the final string to describe the prediction depending on which set of historical matches (home, away of head-to-head)
    list_of_clubs = list(team_df['home_club']) + list(team_df['away_club'])
    num_of_club = len(set(list_of_clubs))
    if num_of_club > 2:
        end_string = 'Score prediction for {} team using {} historic match scores, based on win-loss-draw pattern: '.format(role, role)
    else:
        end_string = 'Score prediction for both teams using head-to-head historic match scores, based on win-loss-draw pattern: '
    
    if skip == True:
        for i in range(team_df.shape[0]):
            if (i%2) == 0:
                team_df.drop(i, inplace=True)
                
        if num_of_club > 2:
            end_string = 'Score prediction for {} team using {} historic match scores after skipping rows, based on win-loss-draw pattern: '.format(role, role)
        else:
            end_string = 'Score prediction for both teams using head-to-head historic match scores after skipping rows, based on win-loss-draw pattern: '
        
    
    overall_cond = [] #The list to compile and check the presence of all three conditions.
    #Check if there is a repeat in score, regardless of team playing a given role or the orientation (home before away)
    scores = []
    for i in range(3):
        temp_scorelist = [list(team_df['home_club_goal'])[i], list(team_df['away_club_goal'])[i]]
        scores.append([max(temp_scorelist), min(temp_scorelist)])
    
    if (scores[0] == scores[1]) | ((scores[0][0] == scores[1][0]) & (abs(scores[0][1] - scores[1][1]) == 1)):
        if scores[2] != scores[0]:
            overall_cond.append('true')
        else:
            overall_cond.append('false')
    else:
        overall_cond.append('false')
        
    #Checks if there roles played by the team were different for the last two games
    roles = [] 
    for i in range(2):
        temp_rolelist = [list(team_df['home_club'])[i], list(team_df['away_club'])[i]]
        roles.append(temp_rolelist.index(team_name))
        
    if len(set(roles)) == 2:
        overall_cond.append('true')
    else:
        overall_cond.append('false')
    
    
    #Checks if the outcome (win, loss or draw) was different for the last two games
    rearranged = []
    #Rearrangement
    for i in range(team_df.shape[0]):
        if list(team_df.iloc[i,:])[2] == team_name:
            rearranged.append(list(team_df.iloc[i,:]))
        else:
            temp_list = list(team_df.iloc[i,:])
            temp_var = temp_list[2]
            temp_list[2] = temp_list[3]
            temp_list[3] = temp_var
            temp_var = temp_list[4]
            temp_list[4] = temp_list[5]
            temp_list[5] = temp_var
            rearranged.append(temp_list)
    new_team_df = pd.DataFrame(rearranged, columns=['date','league','team','opponent','team_score','opponent_score'])
    
    scores = []
    for i in range(3):
        temp_scorelist = [list(new_team_df['team_score'])[i], list(new_team_df['opponent_score'])[i]]
        scores.append(temp_scorelist)
        
    individ_scores = []
    for score in scores[:-1]:
        for individ_score in score:
            individ_scores.append(individ_score)
        
    possible_outcomes = ['win', 'loss', 'draw']
    
    outcomes = []
    for score in scores:
        if score[0] == score[1]:
            outcomes.append('draw')
        elif score[0] > score[1]:
            outcomes.append('win')
        else:
            outcomes.append('loss')
    
    if outcomes[2] in [outcomes[0], outcomes[1]]:
        if outcomes[0] != outcomes[1]:
            overall_cond.append('true')
        else:
            overall_cond.append('false')
    else:
        overall_cond.append('false')
        
    #Compile the final prediction based on whether the three conditions checked for are all present.
    final_output = []
    if 'false' in overall_cond:
        pass
    else:
        prediction = [outcome for outcome in possible_outcomes if outcome not in outcomes]
        if prediction[0] == 'draw':
            elements_counts = Counter(individ_scores)
            final_string = end_string + '{}, {} - {}'.format(prediction[0], elements_counts.most_common()[0][0], elements_counts.most_common()[0][0])
            final_output.append(final_string)
        else:
            elements_counts = Counter(individ_scores)
            final_string = end_string + '{}, {} - {}'.format('win/loss', elements_counts.most_common()[0][0], elements_counts.most_common()[1][0])
            final_output.append(final_string)
    return final_output


def inner_detail_analyser(given_list):
    '''This function takes in the inner match details of the historical matchs of a given team and analyses
    it to extracts certain details to help with prediction.'''
    list_of_analysis = []
    for item in given_list:
        #item_dict = json.loads(item)
        item_dict = item
        inner_detail = {'first_ten_minutes':[], 'last_ten_minutes':[], 'first_half':[],
                        'second_half':[], 'injury_time':[], 'avg_first_ten_minutes':[],
                        'avg_last_ten_minutes':[], 'avg_first_half':[], 'avg_second_half':[],
                        'avg_injury_time':[], 'match_firstgoal':[], 'match_lastgoal':[],
                        'underdog_effect':[]
                       }

        for key in list(item_dict.keys()):
            if item_dict[key] != {}:
                teamgoal_time = item_dict[key]['team']['goal']
                opponentgoal_time = item_dict[key]['opponent']['goal']
                count_firstten = 0
                count_lastten = 0
                count_firsthalf = 0
                count_secondhalf = 0
                count_injurytime = 0

                #Checks which of the goals scored at certain times during the match fit which category
                for minute in teamgoal_time:
                    if int(minute) < 10:
                        inner_detail['first_ten_minutes'].append(minute)
                        count_firstten += 1
                    if int(minute) < 49:
                        inner_detail['first_half'].append(minute)
                        count_firsthalf += 1
                    if int(minute) > 49:
                        inner_detail['second_half'].append(minute)
                        count_secondhalf += 1
                    if int(minute) > 80:
                        inner_detail['last_ten_minutes'].append(minute)
                        count_lastten += 1
                    if (int(minute) > 45) & (int(minute) < 50) | (int(minute) > 90):
                        inner_detail['injury_time'].append(minute)
                        count_injurytime += 1

                #Collates the number of these phenomena per match to later calculate the average
                inner_detail['avg_first_ten_minutes'].append(count_firstten)
                inner_detail['avg_last_ten_minutes'].append(count_lastten)
                inner_detail['avg_first_half'].append(count_firsthalf)
                inner_detail['avg_second_half'].append(count_secondhalf)
                inner_detail['avg_injury_time'].append(count_injurytime)

                team_goal = [int(minute) for minute in teamgoal_time]
                opponent_goal = [int(minute) for minute in opponentgoal_time]

                if (len(team_goal) > 0) & (len(opponent_goal)):
                    #Checks if the team scored the first goal
                    if min(team_goal) < min(opponent_goal):
                        inner_detail['match_firstgoal'].append('1')

                    #Checks if the team scored the last goal
                    if max(team_goal) > max(opponent_goal):
                        inner_detail['match_lastgoal'].append('1')

                    #Checks if the team conceded the first goal but still won the match
                    if (min(team_goal) > min(opponent_goal)) & (len(team_goal) > len(opponent_goal)):
                        inner_detail['underdog_effect'].append('1')  

        inner_detailanalysis = {}
        first_list = ['first_ten_minutes', 'last_ten_minutes', 'first_half', 'second_half', 'injury_time', 
                     'match_firstgoal', 'match_lastgoal', 'underdog_effect', ]
        second_list = ['avg_first_ten_minutes', 'avg_last_ten_minutes', 'avg_first_half', 'avg_second_half', 
                      'avg_injury_time', ]
        for key in inner_detail.keys():
            if key in first_list:
                inner_detailanalysis[key] = len(inner_detail[key])
            if key in second_list:
                inner_detailanalysis[key] = sum(inner_detail[key])/len(inner_detail[key])

        #print(inner_detail)
        list_of_analysis.append(inner_detailanalysis)
    list_of_analysis = pd.DataFrame(list_of_analysis)
    return list_of_analysis


def final_innerdet(innerdetail_df):
    '''This function compares the inner match details of both teams to play, and extracts the dominant team
    for each category.'''
    final_df = {}
    for column in list(innerdetail_df.columns):
        if (max(list(innerdetail_df[column])) == list(innerdetail_df[column])[0]) & (max(list(innerdetail_df[column])) == list(innerdetail_df[column])[1]):
            final_df[column] = ['both teams']
        elif (max(list(innerdetail_df[column])) == list(innerdetail_df[column])[0]):
            final_df[column] = ['home team']
        else:
            final_df[column] = ['away team']
    
    return final_df


def matchscore_total_analysis(dataset, leagues_abbrv):
    '''This function takes in an entire row of the dataset pulled from the database and 
    extracts all the observed pattern from the historic match scores, as well as al the details
    from the inner match details of the historic matches for the home and awar team and their
    head-to-head meatches.'''
    diction = {'7':[1,'home','home_score_patterns'], '8':[2,'away','away_score_patterns'], '9':[1,'home','h2h_score_patterns']}
    columns_list = [7,8,9]
    dict_of_patterns = {'home_score_patterns':[], 'away_score_patterns':[], 'h2h_score_patterns':[],
                        'innerdetail_analysis':[]}
    
    for i in range(dataset.shape[0]):
        row = list(dataset.iloc[i,:])
        for number in columns_list:
            dict_of_pattern = {}
            list_of_pattern = []
            
            #This executes all the analysis functions for pattern finding (skip and no skip) to extract predictions
            try:
                patterns = indiv_teamrole_analysis(row[number], row[diction[str(number)][0]], diction[str(number)][1])
                list_of_pattern = list_of_pattern + [pattern for pattern in patterns]
            except Exception as e:
                except_messgs[f"indiv_teamrole_analysis: {str(i)}"] = f"{type(e).__name__}: {e}" #Catches and Records Error
                list_of_pattern = list_of_pattern + []
            try:
                patterns = indiv_teamrole_analysis(row[number], row[diction[str(number)][0]], diction[str(number)][1], skip=True)
                list_of_pattern = list_of_pattern + [pattern for pattern in patterns]
            except Exception as e:
                except_messgs[f"indiv_teamrole_analysis (skip): {str(i)}"] = f"{type(e).__name__}: {e}" #Catches and Records Error
                list_of_pattern = list_of_pattern + []
            try:
                patterns = indiv_teamrole_analysis(row[number], row[diction[str(number)][0]], diction[str(number)][1], by_league=[row[6],leagues_abbrv[row[6]]])
                list_of_pattern = list_of_pattern + [pattern for pattern in patterns]
            except Exception as e:
                except_messgs[f"indiv_teamrole_analysis (by league): {str(i)}"] = f"{type(e).__name__}: {e}" #Catches and Records Error
                list_of_pattern = list_of_pattern + []
            try:
                patterns = indiv_role_analysis(row[number], diction[str(number)][1])
                list_of_pattern = list_of_pattern + [pattern for pattern in patterns]
            except Exception as e:
                except_messgs[f"indiv_role_analysis: {str(i)}"] = f"{type(e).__name__}: {e}" #Catches and Records Error
                list_of_pattern = list_of_pattern + []
            try:
                patterns = indiv_role_analysis(row[number], diction[str(number)][1], skip=True)
                list_of_pattern = list_of_pattern + [pattern for pattern in patterns]
            except Exception as e:
                except_messgs[f"indiv_role_analysis (skip): {str(i)}"] = f"{type(e).__name__}: {e}" #Catches and Records Error
                list_of_pattern = list_of_pattern + []
            try:
                patterns = indiv_role_analysis(row[number], diction[str(number)][1], by_league=[row[6],leagues_abbrv[row[6]]])
                list_of_pattern = list_of_pattern + [pattern for pattern in patterns]
            except Exception as e:
                except_messgs[f"indiv_role_analysis (by league): {str(i)}"] = f"{type(e).__name__}: {e}" #Catches and Records Error
                list_of_pattern = list_of_pattern + []
            try:
                patterns = indiv_team_analysis(row[number], row[diction[str(number)][0]], diction[str(number)][1])
                list_of_pattern = list_of_pattern + [pattern for pattern in patterns]
            except Exception as e:
                except_messgs[f"indiv_team_analysis: {str(i)}"] = f"{type(e).__name__}: {e}" #Catches and Records Error
                list_of_pattern = list_of_pattern + []
            try:
                patterns = indiv_team_analysis(row[number], row[diction[str(number)][0]], diction[str(number)][1], skip=True)
                list_of_pattern = list_of_pattern + [pattern for pattern in patterns]
            except Exception as e:
                except_messgs[f"indiv_team_analysis (skip): {str(i)}"] = f"{type(e).__name__}: {e}" #Catches and Records Error
                list_of_pattern = list_of_pattern + []
            try:
                patterns = indiv_team_analysis(row[number], row[diction[str(number)][0]], diction[str(number)][1], by_league=[row[6],leagues_abbrv[row[6]]])
                list_of_pattern = list_of_pattern + [pattern for pattern in patterns]
            except Exception as e:
                except_messgs[f"indiv_team_analysis (by league): {str(i)}"] = f"{type(e).__name__}: {e}" #Catches and Records Error
                list_of_pattern = list_of_pattern + []
            try:
                patterns = windrawloss_analysis(row[number], row[diction[str(number)][0]], diction[str(number)][1])
                list_of_pattern = list_of_pattern + [pattern for pattern in patterns]
            except Exception as e:
                except_messgs[f"windrawloss_analysis: {str(i)}"] = f"{type(e).__name__}: {e}" #Catches and Records Error
                list_of_pattern = list_of_pattern + []
            try:
                patterns = windrawloss_analysis(row[number], row[diction[str(number)][0]], diction[str(number)][1], skip=True)
                list_of_pattern = list_of_pattern + [pattern for pattern in patterns]
            except Exception as e:
                except_messgs[f"windrawloss_analysis (skip): {str(i)}"] = f"{type(e).__name__}: {e}" #Catches and Records Error
                list_of_pattern = list_of_pattern + []

            #Adds all the predictions for a given team history to one list to be alter added to the final dataset
            for i in range(len(list_of_pattern)):
                dict_of_pattern[str(i)] = list_of_pattern[i]

            dict_of_patterns[diction[str(number)][2]].append(json.dumps(dict_of_pattern))

        #Analyses the details of the historic matches of the teams to extract relevant information for client.
        try:
            innerdetail_df = inner_detail_analyser(row[-2:])
            innerdetail = final_innerdet(innerdetail_df)
            dict_of_patterns['innerdetail_analysis'].append(json.dumps(innerdetail))
        except Exception as e:
            except_messgs[f"innerdetail_analysis: {str(i)}"] = f"{type(e).__name__}: {e}" #Catches and Records Error
            dict_of_patterns['innerdetail_analysis'].append(json.dumps({}))
    return dict_of_patterns


def filter_pred(dataset):
    '''This function filters the final dataset gotten from the analysis function to check for and retain
    only interesting predictions as specified by the client.'''
    
    index_list = list(dataset.index)
    limit = dataset.shape[0]
    dataset_2 = dataset.copy(deep=True)
    
    for i in range(limit):
        row = list(dataset.iloc[i,:])
        pattern_list = row[-4:-1]
        count_1 = 0
        count_2 = 0
        count_3 = 0
        for pattern_dict in pattern_list:
            pattern_dict = json.loads(pattern_dict)
            if pattern_dict != {}:
                for key in pattern_dict.keys():
                    score = pattern_dict[key][-5:]
                    score = score.replace(' ','')
                    score = score.split('-')
                    score = [int(elem) for elem in score]
                    if sum(score) >= 2: #Checks the first condition to see if the patterns are interesting
                        count_2 += 1
                    if (score[0] >= 1) & (score[1] >= 1): #checks the second condition for interesting patterns only
                        count_3 += 1
                    count_1 += 1
                    
        #If enough interesting patterns are observed, the row is retained, else row is discarded.
        if count_1 > 3:
            if ((count_2/count_1) > 0.70) | ((count_3/count_1) > 0.70):
                pass
            else:
                dataset_2.drop(index_list[i], axis=0, inplace=True)
        elif (count_1 <= 3) & (count_1 > 0):
            if ((count_2/count_1) > 0.60) | ((count_3/count_1) > 0.60):
                pass
            else:
                dataset_2.drop(index_list[i], axis=0, inplace=True)
        else:
            dataset_2.drop(index_list[i], axis=0, inplace=True)
    return dataset_2


def pred_counter(filtered_dataset, threshold):
        '''This function calculates the number of predictions per league and 
        also records matches with less than 5 prediction'''
        
        pred_threshold = threshold
        leagues_list = list(set(list(filtered_dataset['league']))) #Get list of leagues with prediction
        predcount_dict = {} #dictionay to be returned is created
        for league in leagues_list: #Loops through the list of leagues and count predicitons
            predcount_dict[league] = []
            temp_df = filtered_dataset[filtered_dataset['league'] == league]
            predcount_dict[league].append(f"{league}: {temp_df.shape[0]}")
            for i in range(temp_df.shape[0]): #For each match in a league the number of prediction are counted and checked against a threshold.
                temp_list = list(temp_df.iloc[i,:])
                count = 0
                for index in [12,13,14]:
                    count = count + len(list(json.loads(temp_list[index]).keys()))
                if count <= pred_threshold: #If number of predictions is leass than threshold, match is recorded
                    predcount_dict[league].append(f"{temp_list[0]}:{temp_list[1]} - {temp_list[2]}; Prediction: {count}")
        return predcount_dict


def teamdata_loader(dataset):
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
    create_query = '''CREATE TABLE IF NOT EXISTS match_prediction (
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
        innerdetail_analysis JSONB
    );'''
    cursor.execute(create_query)
    connection.commit()

    # Create a SQLAlchemy engine
    engine = create_engine(f'postgresql+psycopg2://{settings.database_user}:{settings.database_password}@{settings.database_hostname}/{settings.database_name}')

    dataset.to_sql('match_prediction', engine, if_exists='append', index=False)

    #Commit and close connection
    connection.commit()
    cursor.close()
    connection.close()


def team_analysis_flow(today, tomorrow):
    #Team Match Analysis
    for i in range(2): #Tries twice to extract the data from the database
        try:
            match_df = teamdata_extract() #If try is successful, breaks the loop
            break
        except Exception as e:
            except_messgs[f"(Database Extraction): {str(i)}"] = f"{type(e).__name__}: {e}" #Catches and Records Error
    
    try:
        #Converts the date column for filtering
        match_df['date'] = pd.to_datetime(match_df['date'], format="%Y-%m-%d %H:%M:%S") #  format='%d/%m/%Y')
        today_df = match_df[(match_df['date'].dt.date == today) | (match_df['date'].dt.date == tomorrow)] #Account for when the dataset filter everything due to no matching date
        today_df = today_df.copy(deep=True)
    except Exception as e:
        except_messgs[f"(Data Wrangling)"] = f"{type(e).__name__}: {e}" #Catches and Records Error
    
    try:
        #Analyses the dataset and generates the predictions
        additional_columns = matchscore_total_analysis(today_df, leagues_abbrv)
    except Exception as e:
        except_messgs[f"(Data Analysis)"] = f"{type(e).__name__}: {e}" #Catches and Records Error
    
    try:
        modified_dataset = today_df.copy(deep=True)
        #Adds the prediction to the original dataset
        for key in additional_columns.keys():
            modified_dataset[key] = additional_columns[key]
    except Exception as e:
        except_messgs[f"(Prediction Addition)"] = f"{type(e).__name__}: {e}" #Catches and Records Error
    
    try:
        #Filters the perdiction only retaining the "interesting" predicitons
        filtered_dataset = filter_pred(modified_dataset)
    except Exception as e:
        except_messgs[f"(Prediction Filtering)"] = f"{type(e).__name__}: {e}" #Catches and Records Error
    
    try:
        #Converts the columns with dictionaries to json format for loading
        col_list = ['home_team_matches','away_team_matches','head2head_matches','home_team_matchespattern','away_team_matchespattern']
        for column in col_list:
            filtered_dataset[column] = filtered_dataset[column].apply(json.dumps)
    except Exception as e:
        except_messgs[f"(Datatype Transformation)"] = f"{type(e).__name__}: {e}" #Catches and Records Error
    
    #pred_diction = dict(Counter(list(filtered_dataset['league'])))
    
    #This function calculates the number of predictions per league and also records matches with less than 5 prediction
    pred_diction = pred_counter(filtered_dataset, 5)
    for key in list(pred_diction.keys()):
        pred_count[key] = pred_diction[key]
    
    try:
        teamdata_loader(filtered_dataset)
    except Exception as e:
        except_messgs[f"(Database Loading)"] = f"{type(e).__name__}: {e}" #Catches and Records Error
    


