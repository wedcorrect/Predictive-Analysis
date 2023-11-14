from datetime import date, timedelta
from team_analysis import team_analysis_flow, except_messgs, pred_count
from ref_analysis import ref_analysis_flow, refexcept_messgs
from rules_check import rules_check, rulesexcept_messgs
from ml_predict import ml_predictor
from config import settings
import smtplib, os
from email.message import EmailMessage

def main():
    '''Main function the runs the entire app workflow, from extraction of the individual team's
    match history to the referee's history from the database to analysing this data and making
    predictions. These predictions are then filtered and loaded into the database for viewing.
    
    It also records the error log and predictions and sends it to the dev's/client's email'''
    today = date.today()
    tomorrow = date.today() + timedelta(days=1)
    print(today, tomorrow)

    if (today.day % 2) == 0:
        team_analysis_flow(today, tomorrow)
        ref_analysis_flow(today, tomorrow)
        rules_check()

        #Concatenating error logs to send to email.
        email_1 = f"Error Logs for {today} and {tomorrow} Analysis.\n\n"
        email_1 = email_1 + f"Teams' Analysis\n"
        for item in list(except_messgs.keys()):
            if item == list(except_messgs.keys())[-1]:
                email_1 = email_1 + f"{item}: {except_messgs[item]}\n\n"
            else:
                email_1 = email_1 + f"{item}: {except_messgs[item]}\n"
        email_1 = email_1 + f"Referee's Analysis\n"
        for item in list(refexcept_messgs.keys()):
            if item == list(refexcept_messgs.keys())[-1]:
                email_1 = email_1 + f"{item}: {refexcept_messgs[item]}\n\n"
            else:
                email_1 = email_1 + f"{item}: {refexcept_messgs[item]}\n"
        
        #Sends error message to Email for recording or review
        msg_1 = EmailMessage()
        msg_1['Subject'] = f"Error Logs for {today} and {tomorrow} Analysis."
        msg_1['From'] = settings.email_address
        msg_1['To'] = "michaeligbomezie@gmail.com"
        msg_1.set_content(email_1)

        #Concatenating the prediction for client's update.
        email_2 = f"Predictions per League for {today} and {tomorrow}.\n\n"
        for item in list(pred_count.keys()):
            if item == list(pred_count.keys())[-1]:
                email_2 = email_2 + f"{item}: {pred_count[item]}\n\n"
            else:
                email_2 = email_2 + f"{item}: {pred_count[item]}\n"
        
        #Sends error message to Email for recording or review
        msg_2 = EmailMessage()
        msg_2['Subject'] = f"Predictions per League for {today} and {tomorrow}."
        msg_2['From'] = settings.email_address
        msg_2['To'] = "dexterhardeveld@live.nl"
        msg_2.set_content(email_2)

        #Concatenating error logs to send to email.
        email_3 = f"Error Logs for {today} and {tomorrow} Rules_Check.\n\n"
        for item in list(rulesexcept_messgs.keys()):
            if item == list(rulesexcept_messgs.keys())[-1]:
                email_3 = email_3 + f"{item}: {rulesexcept_messgs[item]}\n\n"
            else:
                email_3 = email_3 + f"{item}: {rulesexcept_messgs[item]}\n"
        
        #Sends error message to Email for recording or review
        msg_3 = EmailMessage()
        msg_3['Subject'] = f"Error Logs for {today} and {tomorrow} Rules_Check."
        msg_3['From'] = settings.email_address
        msg_3['To'] = "michaeligbomezie@gmail.com"
        msg_3.set_content(email_3)

        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(settings.email_address, settings.email_password)
            smtp.send_message(msg_1)
            smtp.send_message(msg_2)
            smtp.send_message(msg_3)
    
    ml_predictor(today, today)


if __name__ == '__main__':
    dyno_type = os.environ.get("DYNO")
    print(dyno_type)
    if ('run' in dyno_type) | ('scheduler' in dyno_type):
        main()