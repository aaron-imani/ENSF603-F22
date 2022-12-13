from datetime import datetime
import json
import boto3
from os import environ
# import logging
import pytz
import itertools
import aioboto3
import botocore
import asyncio

# Set up our logger
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger()

# Timezone
canada = pytz.timezone('US/Mountain')
now = None

session = aioboto3.Session()
# ses = session.client('ses')

# Get the service resource.
dynamodb = boto3.resource('dynamodb')

base_url = environ['AR_BASE_URL']
team_name = environ['AR_TEAM_NAME']
source_email = environ['AR_SOURCE_EMAIL']
meeting_url = '/Mother/meeting/'


async def list_items(table_name: str):
    try:
        async with session.resource('dynamodb', region_name=environ['REGION']) as dynamo_resource:
            table = await dynamo_resource.Table(table_name)
            data = await table.scan()
            return data
    except Exception as e:
        return e


async def _get_item(table_name, id):
    try:
        print('get info for user id ', id)
        async with session.resource('dynamodb', region_name=environ['REGION']) as dynamo_resource:
            table = await dynamo_resource.Table(table_name)
            data = await table.get_item(Key={'id': id})
            return data.get('Item')
    except Exception as e:
        return e


def get_table_name(readable_name: str, env_name: str):
    table_id = env_name[:env_name.index(':')]
    return f'{readable_name}-{table_id}-{environ["ENV"]}'


async def _get_study_case_roles(table_name: str, study_case_ID: str):
    print('get_study_case_roles studyCaseID =', study_case_ID)

    try:
        query_params = {
            'TableName': table_name,
            'FilterExpression': 'studyCaseID = :studyCaseID',
            'ExpressionAttributeValues': {
                ':studyCaseID': study_case_ID
            }
        }
        async with session.resource('dynamodb', region_name=environ['REGION']) as dynamo_resource:
            table = await dynamo_resource.Table(table_name)
            data = await table.scan(**query_params)
            data = data.get('Items')
            print('get_study_case_roles result = ' + json.dumps(data))
            return data

    except Exception as e:
        print(e)
        return e


def _isVisitValid(visit):
    try:
        if visit['status'] != 'HAS_ENDED' and visit['status'] != 'CANCELED':
            try:
                start_time = datetime.fromisoformat(visit['startTime'])
            except ValueError:
                start_time = datetime.strptime(
                    visit['startTime'], "%Y-%m-%dT%H:%M:%S.%fZ").astimezone(canada)

            diff_minutes = (now-start_time).total_seconds()/60
            print('--------> comparing, ', start_time, ' with ',
                  now, '. Result:', diff_minutes, 'minutes')
            if (start_time > now and diff_minutes <= 20):
                print(
                    '--------> Visit is less than twenty minutes in filtering, visit =', visit)
                return True
            else:
                print('--------> Visit is NOT less than twenty minutes in the future. Time diff:', diff_minutes, ', visit = ',
                      visit)

            return False

    except Exception as e:
        print('--------> Exception in filtering visits:', e)

    return True


async def _get_meeting_attendees(visit):
    print('parentPromises visit =', visit)
    params = {
        'IndexName': 'byMeeting',
        'KeyConditionExpression': 'meetingID = :meetingID',
        'ExpressionAttributeValues': {
            ':meetingID': visit['id']
        },
    }

    async with session.resource('dynamodb', region_name=environ['REGION']) as dynamo_resource:
        table = await dynamo_resource.Table(environ['API_VIDKIDS_MEETING_ATTENDEE_TABLE_NAME'])
        meeting_attendees = await table.query(**params)

    print('parentPromises meetingAttendees =', meeting_attendees)

    return [await _get_item(environ['API_VIDKIDS_USERTABLE_NAME'], attendee['userID']) for attendee in meeting_attendees['Items']]


async def _get_other_nurses(visit):
    study_case_roles = await _get_study_case_roles(
        environ['API_VIDKIDS_STUDYCASEROLETABLE_NAME'], visit['studyCaseID'])

    other_nurses = []

    if study_case_roles != None:
        for study_case_role in study_case_roles:
            if study_case_role['role'] != 'NURSE':
                continue

            from_date = study_case_role['fromDate']
            to_date = study_case_role['toDate']

            from_date_time = datetime.fromisoformat(
                from_date if from_date else datetime.min)
            to_date_time = datetime.fromisoformat(
                to_date if to_date else datetime.max)

            print('check if starter is temporary : ' +
                study_case_role['userID'] +
                ', from = ' +
                from_date_time +
                ', to = ' +
                to_date_time +
                ', now = ' +
                now)

            if now <= to_date_time and now >= from_date_time:
                user = await _get_item(environ['API_VIDKIDS_USERTABLE_NAME'], study_case_role['userID'])
                other_nurses.append((user, visit))
            else:
                print('skip send email to ', study_case_role['userID'])

    return other_nurses


async def _send_email_to_attendees(attendees, visits, is_nurse):
    emails = [await _send_email_to_attendee(attendee, visits[index], is_nurse)
              for index, attendee in enumerate(attendees)]
    return emails


async def validate(event, context):
    print('event =', event)
    global now
    now = datetime.now(tz=canada)

    all_visits = await list_items(environ['API_VIDKIDS_MEETINGTABLE_NAME'])
    print('-------------> ', all_visits)

    valid_future_visits = list(filter(_isVisitValid, all_visits['Items']))
    print('--------> Valid future visits: ', valid_future_visits)

    if len(valid_future_visits) == 0:
        print('--------> Empty Valid future visits')
        return event

    try:
        meeting_attendees_per_visit = [await _get_meeting_attendees(visit) for visit in valid_future_visits]

        # Flattening the parents_per_visit list
        meetings_attendees = list(
            itertools.chain(*meeting_attendees_per_visit))

        print('parent =', meetings_attendees)

        # starter_nurses = [await _get_item(
        #     environ['API_VIDKIDS_USERTABLE_NAME'], visit['starterID']) for visit in valid_future_visits]

        now = datetime.now(tz=canada)
        other_nurses = [await _get_other_nurses(visit) for visit in valid_future_visits]

        # Flattening the other_nurses list
        other_nurses = list(itertools.chain(*other_nurses))

        print('delegatedUsers : ', other_nurses)

        email_parents = await _send_email_to_attendees(meetings_attendees, valid_future_visits, False)
        email_results = email_parents

        if len(other_nurses) > 0:
            other_nurses_users, other_nurses_visits = [
            (user, visit) for (user, visit) in other_nurses]
            email_other_nurses = await _send_email_to_attendees(other_nurses_users, other_nurses_visits, True)
            email_parents.extend(email_other_nurses)

        # email_starter_nurses = await _send_email_to_attendees(starter_nurses, valid_future_visits, True)


        # email_parents.extend(email_starter_nurses)

        print('--------> emailResult', email_results)

    except Exception as e:
        print('--------> Exception in requesting parent info: ', e)

    return event


def meeting_reminder(event, context):
    return asyncio.get_event_loop().run_until_complete(validate(event, context))


async def _send_email_to_attendee(attendee, visit, is_nurse):
    print('send email for ', visit, attendee)
    try:
        visit_time = datetime.fromisoformat(visit['startTime'])
    except ValueError:
        visit_time = datetime.strptime(visit['startTime'], "%Y-%m-%dT%H:%M:%S.%fZ").astimezone(canada)

    formatted_time = visit_time.strftime('%H:%M %p')

    if is_nurse:
        message = f'''Dear {attendee["name"]}\n\n
                      Your {team_name} Virtual visit is about to start.\n\n
                      Please join the visit at {formatted_time}.\n\n
                      {team_name} Virtual Team'''
    else:
        message = f'''Dear {attendee["name"]}\n\n
        This is a friendly reminder that your {team_name} meeting 
        will start shortly, at {formatted_time}.\n\n
        Please go to the following link to start your visit:\n
        {base_url}{meeting_url}{visit["id"]}\n\n
        If the link above does not work, please try copying it into your web browser.\n\n
        Thank you,\n
        {team_name} Virtual Team
        '''

    status = await _send_email(attendee['email'], message)
    return status


async def _send_email(to, body):
    email_parameters = {
        'Destination': {
            'ToAddresses': [to],
        },
        'Message': {
            'Body': {
                'Text': {
                    'Data': body,
                },
            },
            'Subject': {
                'Data': 'Reminder that visit starting soon',
            },
        },
        'Source': source_email,
    }

    print(f'EVENT: {json.dumps(email_parameters)}')
    
    try:
        async with session.client('ses') as ses:
            email = await ses.send_email(**email_parameters)

            if 'MessageId' in email:
                print('===EMAIL SENT===')
                return 'Email sent'

    except botocore.exceptions.ClientError as error:
        print(error)
        return error

    print(email)
    print('EMAIL CODE END')
